import os
import json
from playwright.sync_api import Playwright, sync_playwright
from urllib.parse import parse_qs, urlparse
import pyotp
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

UPSTOX_API_KEY = os.getenv('UPSTOX_API_KEY')
UPSTOX_API_SECRET = os.getenv('UPSTOX_API_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
MOBILE_NO = os.getenv('MOBILE_NO')
PIN = os.getenv('PIN')
TOTP_KEY = os.getenv('TOTP_KEY')

login_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={UPSTOX_API_KEY}&redirect_uri={REDIRECT_URI}"


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
    """Fetch a new access token using the automated authentication flow."""
    print("🔄 Access token not found or expired. Initiating authentication...")

    # Run Playwright to get the authorization code
    with sync_playwright() as playwright:
        auth_code = run(playwright)  # Automate login and get the code

    if not auth_code:
        print("❌ Failed to retrieve authorization code.")
        exit()

    # Exchange authorization code for access token
    token_url = "https://api.upstox.com/v2/login/authorization/token"
    payload = {
        'code': auth_code,
        'client_id': UPSTOX_API_KEY,
        'client_secret': UPSTOX_API_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }

    response = requests.post(token_url, data=payload)
    access_token_info = response.json()

    if 'access_token' in access_token_info:
        new_token = access_token_info['access_token']
        print(f"✅ New Access Token: {new_token}")
        save_access_token(new_token)  # Update .env file
        return new_token
    else:
        print(f"❌ Error retrieving new access token: {access_token_info}")
        exit()


def run(playwright: Playwright) -> str:
    """Automate the login process and extract authorization code."""
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()

    try:
        # Go to the Upstox login page
        page.goto(login_url)

        # Enter mobile number and get OTP
        page.locator("#mobileNum").fill(MOBILE_NO)
        page.get_by_role("button", name="Get OTP").click()

        # Generate and enter OTP
        otp = pyotp.TOTP(TOTP_KEY).now()
        page.locator("#otpNum").fill(otp)
        page.get_by_role("button", name="Continue").click()

        # Enter PIN
        page.get_by_role("textbox", name="Enter 6-digit PIN").fill(PIN)
        page.get_by_role("button", name="Continue").click()

        # Wait for redirection and capture the URL
        page.wait_for_function("window.location.href.includes('?code=')", timeout=60000)
        redirected_url = page.evaluate("window.location.href")

        # Extract the authorization code
        parsed_url = urlparse(redirected_url)
        auth_code = parse_qs(parsed_url.query).get('code', [None])[0]

        if auth_code:
            print(f"✅ Extracted Authorization Code: {auth_code}")
            return auth_code
        else:
            print("❌ Authorization code not found!")
            exit()

    except Exception as e:
        print(f"❌ Error during login process: {e}")
        exit()

    finally:
        context.close()
        browser.close()


def fetch_user_profile(token):
    """Fetch the user profile using the provided access token."""
    profile_url = "https://api.upstox.com/v2/user/profile"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    response = requests.get(profile_url, headers=headers)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 401:  # Unauthorized - token might be expired
        print("⚠️ Access token expired. Fetching a new one...")
        return None
    else:
        print(f"❌ Error fetching profile: {response.json()}")
        exit()


# Step 1: Try using the existing access token
if ACCESS_TOKEN:
    user_profile = fetch_user_profile(ACCESS_TOKEN)
    if not user_profile:  # Token expired
        ACCESS_TOKEN = get_new_access_token()  # Get new token
        user_profile = fetch_user_profile(ACCESS_TOKEN)  # Retry
else:
    ACCESS_TOKEN = get_new_access_token()  # Get token if none exists
    user_profile = fetch_user_profile(ACCESS_TOKEN)  # Fetch profile

# Step 2: Display user profile
if user_profile:
    print("✅ User Profile:", json.dumps(user_profile, indent=4))

print(ACCESS_TOKEN)
