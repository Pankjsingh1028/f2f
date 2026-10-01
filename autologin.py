"""Upstox auto-login over plain HTTP (no Playwright).

Same flow as autologin.py, but instead of driving a headless browser it calls the
endpoints the login SPA itself calls. Paths/payloads below were read out of the
login.upstox.com JS bundle and verified against the live service:

    1. GET  api.upstox.com/v2/login/authorization/dialog
       -> redirects to login.upstox.com carrying client_id / redirect_uri / user_id
    2. POST service.upstox.com/login/open/v6/auth/1fa/otp/generate
       -> {"data": {mobileNumber, userId, countryIsdCode}}, returns validateOTPToken
    3. POST service.upstox.com/login/open/v5/auth/1fa/otp-totp/verify
       -> {"data": {otp, validateOtpToken}}, returns accounts[] (profileId, userId)
    4. POST service.upstox.com/login/open/v4/auth/2fa
       -> {"data": {twoFAMethod: "SECRET_PIN", inputText: base64(PIN), profileId: int}}
          with X-Profile-Id / X-User-Id headers and ?client_id=&redirect_uri=
    5. follow data.redirectUri -> lands on REDIRECT_URI carrying ?code=...

Notes:
  * Step 2 needs the `user_id` from the step-1 redirect URL. Without it the service
    answers with the generic error 1017016.
  * The 6-digit TOTP from TOTP_KEY is accepted in place of the SMS OTP
    (the generate response reports isTotpEnabled: true).
  * The PIN is base64-encoded before it is sent (the SPA does `btoa(secretPin)`);
    sending it in plain text comes back as "incorrect PIN".
  * The PIN endpoint locks the account after 5 wrong attempts, so a wrong PIN is
    surfaced immediately and the run aborts rather than retrying.

Set AUTOLOGIN_DEBUG=1 to dump every request/response while debugging.
"""

import os
import base64
import uuid
import json
import re
from urllib.parse import parse_qs, urlparse

import sys

import pyotp
import requests
from dotenv import load_dotenv

# Windows consoles/pipes default to cp1252, which cannot encode the status emoji.
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except (AttributeError, ValueError):
    pass

load_dotenv()

UPSTOX_API_KEY = os.getenv('UPSTOX_API_KEY')
UPSTOX_API_SECRET = os.getenv('UPSTOX_API_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
MOBILE_NO = os.getenv('MOBILE_NO')
PIN = os.getenv('PIN')
TOTP_KEY = os.getenv('TOTP_KEY')
COUNTRY_ISD_CODE = os.getenv('COUNTRY_ISD_CODE', '+91')

DEBUG = os.getenv('AUTOLOGIN_DEBUG') == '1'

LOGIN_URL = (
    "https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code&client_id={UPSTOX_API_KEY}&redirect_uri={REDIRECT_URI}"
)
LOGIN_BASE = "https://service.upstox.com/login/open"
OTP_GENERATE_PATH = "/v6/auth/1fa/otp/generate"   # individual-API variant of /v9/...
OTP_VERIFY_PATH = "/v5/auth/1fa/otp-totp/verify"
TWO_FA_PATH = "/v4/auth/2fa"
OAUTH_BASE = "https://service.upstox.com/login/v2/oauth"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


class LoginError(RuntimeError):
    """Raised when a step of the login flow fails."""


def _debug(label, response):
    if not DEBUG:
        return
    print(f"\n--- {label} :: {response.request.method} {response.url} -> {response.status_code}")
    print(response.text[:1500])


def _json(response):
    try:
        return response.json()
    except ValueError:
        return {}


def _check(response, label):
    """Upstox answers 200 with {"success": false, "error": {...}} on failure."""
    payload = _json(response)
    if response.status_code >= 400 or payload.get('success') is False:
        error = payload.get('error') or {}
        message = error.get('message') or response.text[:300]
        code = error.get('code')
        raise LoginError(f"{label} failed [{response.status_code}/{code}]: {message}")
    return payload


def _find_auth_code(*blobs):
    """Pull ?code=... out of any URL hiding in a response body / header."""
    for blob in blobs:
        if not blob:
            continue
        if not isinstance(blob, str):
            blob = json.dumps(blob)
        match = re.search(r'[?&]code=([A-Za-z0-9\-._~]+)', blob)
        if match:
            return match.group(1)
    return None


def new_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': USER_AGENT,
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/json',
        'Origin': 'https://login.upstox.com',
        'Referer': 'https://login.upstox.com/',
        'x-device-details': 'platform=WEB|osName=Windows/10|osVersion=Chrome/124.0.0.0|'
                            'appVersion=4.0.0|modelName=Chrome',
    })
    return session


def get_auth_code() -> str:
    """Walk the Upstox login flow with plain HTTP calls and return the auth code."""
    session = new_session()

    # Step 1: open the OAuth dialog. It redirects to login.upstox.com with the
    # params the login SPA needs (and seeds the session cookies).
    dialog = session.get(LOGIN_URL, timeout=30)
    _debug("dialog", dialog)

    params = parse_qs(urlparse(dialog.url).query)
    client_id = params.get('client_id', [None])[0]
    redirect_uri = params.get('redirect_uri', [None])[0]
    individual_api_user_id = params.get('user_id', [None])[0]
    if not (client_id and redirect_uri and individual_api_user_id):
        raise LoginError(
            f"Login dialog did not return the expected params (got {sorted(params)}). "
            "Check UPSTOX_API_KEY / REDIRECT_URI."
        )
    oauth_query = {'client_id': client_id, 'redirect_uri': redirect_uri}

    # Step 2: start the OTP challenge. userId is required for this variant.
    generate = session.post(
        f"{LOGIN_BASE}{OTP_GENERATE_PATH}",
        json={'data': {
            'mobileNumber': MOBILE_NO,
            'userId': individual_api_user_id,
            'countryIsdCode': COUNTRY_ISD_CODE,
        }},
        timeout=30,
    )
    _debug("otp/generate", generate)
    generate_data = _check(generate, "OTP generation").get('data') or {}
    validate_token = generate_data.get('validateOTPToken')
    if not validate_token:
        raise LoginError(f"OTP generation returned no validateOTPToken: {generate.text[:300]}")

    # Step 3: verify the TOTP derived from TOTP_KEY (accepted in place of the SMS OTP).
    otp = pyotp.TOTP(TOTP_KEY).now()
    verify = session.post(
        f"{LOGIN_BASE}{OTP_VERIFY_PATH}",
        json={'data': {'otp': otp, 'validateOtpToken': validate_token}},
        timeout=30,
    )
    _debug("otp/verify", verify)
    verify_data = _check(verify, "OTP verification").get('data') or {}

    accounts = verify_data.get('accounts') or []
    if not accounts:
        raise LoginError(f"OTP verification returned no accounts: {verify.text[:300]}")
    account = next((a for a in accounts if a.get('isDefault')), accounts[0])
    profile_id = str(account.get('profileId'))
    user_id = account.get('userId')

    # Step 4: submit the 6-digit PIN, base64-encoded the way the SPA does it.
    # Five wrong attempts lock the account, so a rejected PIN aborts the run
    # instead of being retried.
    two_fa = session.post(
        f"{LOGIN_BASE}{TWO_FA_PATH}",
        params=oauth_query,
        headers={'X-Profile-Id': profile_id, 'X-User-Id': user_id},
        json={'data': {
            'twoFAMethod': 'SECRET_PIN',
            'inputText': base64.b64encode(PIN.encode()).decode(),
            'profileId': int(profile_id),
        }},
        timeout=30,
        allow_redirects=False,
    )
    _debug("2fa", two_fa)
    two_fa_payload = _check(two_fa, "PIN verification")

    # Step 5: follow data.redirectUri until something carries ?code=.
    auth_code = _find_auth_code(two_fa_payload, two_fa.headers.get('Location'))
    if auth_code:
        return auth_code

    data = two_fa_payload.get('data') or {}
    next_url = next(
        (data[key] for key in ('redirectUri', 'redirectUrl', 'redirect_uri') if data.get(key)),
        None,
    )

    # For an external OAuth client the 2FA response carries redirectUri: null and
    # the approval has to be posted separately (the SPA's startOauth()).
    if not next_url:
        approve = session.post(
            f"{OAUTH_BASE}/authorize",
            params={
                'client_id': client_id,
                'response_type': 'code',
                'redirect_uri': redirect_uri,
                'requestId': uuid.uuid4().hex,
            },
            json={'data': {'userOAuthApproval': True}},
            timeout=30,
            allow_redirects=False,
        )
        _debug("oauth/authorize", approve)
        approve_data = _check(approve, "OAuth approval").get('data') or {}
        next_url = approve_data.get('redirectUri')
        if not next_url:
            raise LoginError(f"OAuth approval returned no redirectUri: {approve.text[:300]}")

    if not next_url:
        raise LoginError(f"2FA succeeded but returned no redirectUri: {two_fa.text[:300]}")

    for _ in range(10):
        hop = session.get(next_url, allow_redirects=False, timeout=30)
        _debug("redirect", hop)
        location = hop.headers.get('Location')
        auth_code = _find_auth_code(location, hop.url, _json(hop))
        if auth_code:
            return auth_code
        if not location:
            break
        next_url = requests.compat.urljoin(hop.url, location)

    raise LoginError(
        "Authorization code not found. Re-run with AUTOLOGIN_DEBUG=1 to inspect the responses."
    )


def save_access_token(token):
    """Save the new access token to the .env file."""
    with open(".env", "r") as env_file:
        lines = env_file.readlines()

    with open(".env", "w") as env_file:
        found = False
        for line in lines:
            if line.startswith("ACCESS_TOKEN="):
                env_file.write(f"ACCESS_TOKEN={token}\n")
                found = True
            else:
                env_file.write(line)

        if not found:
            env_file.write(f"\nACCESS_TOKEN={token}\n")

    print("✅ New access token saved to .env")


def get_new_access_token():
    """Fetch a new access token using the HTTP-only authentication flow."""
    print("🔄 Access token not found or expired. Initiating authentication...")

    try:
        auth_code = get_auth_code()
    except LoginError as exc:
        print(f"❌ Error during login process: {exc}")
        raise SystemExit(1)

    print(f"✅ Extracted Authorization Code: {auth_code}")

    token_url = "https://api.upstox.com/v2/login/authorization/token"
    payload = {
        'code': auth_code,
        'client_id': UPSTOX_API_KEY,
        'client_secret': UPSTOX_API_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
    }

    response = requests.post(token_url, data=payload, timeout=30)
    access_token_info = response.json()

    if 'access_token' in access_token_info:
        new_token = access_token_info['access_token']
        print(f"✅ New Access Token: {new_token}")
        save_access_token(new_token)
        return new_token

    print(f"❌ Error retrieving new access token: {access_token_info}")
    raise SystemExit(1)


def fetch_user_profile(token):
    """Fetch the user profile using the provided access token."""
    profile_url = "https://api.upstox.com/v2/user/profile"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    response = requests.get(profile_url, headers=headers, timeout=30)

    if response.status_code == 200:
        return response.json()
    if response.status_code == 401:  # Unauthorized - token might be expired
        print("⚠️ Access token expired. Fetching a new one...")
        return None

    print(f"❌ Error fetching profile: {response.text}")
    raise SystemExit(1)


def main():
    token = ACCESS_TOKEN
    user_profile = fetch_user_profile(token) if token else None

    if not user_profile:
        token = get_new_access_token()
        user_profile = fetch_user_profile(token)

    if user_profile:
        print("✅ User Profile:", json.dumps(user_profile, indent=4))

    print(token)
    return token


if __name__ == "__main__":
    main()
