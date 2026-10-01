# Changelog

## 2026-09-11

### Fixed — `mwpl.py` utilisation was overstated; NSE measures it on the futures leg

IREDA read 95% against NSE's published 81.72%. Reconciled live against
`combineoi.csv` dated 10-SEP-2026 — first on IREDA, then across all 211
symbols × 3 expiries in a single pass.

- **Raw OI collection was already correct**, which is what made the rest
  diagnosable. Our futures+options total comes in at a median 100.4% of NSE's
  `Open Interest` column (p10 99.4%, p90 102.6%), so units, lot handling and
  universe were never the problem and the weighting was the only free variable.

- **`DELTA_MODE` is gone; `FUTEQ_MODE` replaces it, defaulting to `"futures"`.**
  Despite its name, NSE's `Future Equivalent Open Interest` tracks the *futures*
  open interest, not a delta-weighted total. Scaling each symbol's legs back by
  its own one-session drift (our total OI / NSE's, median 1.0036):

  | | bias | \|err\| median | \|err\| p90 | within 5pp |
  |---|---|---|---|---|
  | futures only | −0.99 | **0.95** | 2.75 | **98%** |
  | futures + signed options | −2.23 | 2.05 | 6.65 | 84% |
  | futures + abs options | +6.84 | 5.58 | 13.72 | 45% |

  Errors are percentage points of MWPL, n=208. `NSE futEq / futures OI` has
  median 1.0345 (p10 0.9963, p90 1.0902). **Adding a delta-weighted option leg
  in either sign convention makes the match worse**, so utilisation is measured
  on the futures leg.

  The option book is not literally ignored — the implied contribution is about
  +5% of option OI (p10 −0.3%, p90 +10.2%) — but that is far below what either
  convention produces (our chains: −7.8% of option OI signed, +33.8% absolute).
  The exact treatment is not recoverable from this file. A fitted constant
  (`futures + 0.0339 * optionOI`, |err| median 0.54) buys 0.4pp from a free
  parameter, so it is documented in the module docstring rather than shipped.

  `"signed"` and `"abs"` remain selectable. Both option legs are computed in the
  same pass and reported as `optDelta` / `optDeltaAbs` regardless of the mode, so
  switching costs no extra API calls.

  Absolute delta — the original default, annotated "NSE's standard" — overstated
  **every one** of the 208 symbols (minimum error +1.15pp) and is what put IREDA
  at 95%.

- **As shipped, against all 210 NSE rows:** bias −0.73pp, median |error| 0.81pp,
  p90 2.21pp, 98.6% within 5pp, 100% within 10pp. All five of NSE's
  "No Fresh Positions" names hold above the 80% exit floor (SAIL 95.6,
  BANDHANBNK 92.8, MANAPPURAM 89.2, KAYNES 87.4, INOXWIND 83.2) with no false
  ban trips. Worst outlier is IREDA at +9.28pp, where futures OI rose ~11% in
  one session while total OI was flat.

### Fixed — `mwpl.py` expiry coverage

- **Widening `EXPIRIES` never widened the futures leg.** `load_universe` pinned a
  single `min(futs[s])` key and `fetch_futures_oi` read only that, so the
  documented "widen `EXPIRIES` to match NSE" fix left far-month *futures* out no
  matter what. IREDA carries 17% of its futures OI in Oct/Nov (89.6M near vs
  108.3M total). `universe[s]["futs"]` is now a list; every contract is batched
  and summed.
- **`EXPIRIES` defaults to all three months.** Cost is ~633 option/chain calls
  plus 2 batched quote calls per sweep, so `POLL_INTERVAL_SEC` moves 240s → 600s
  to stay inside the 2000 req/30min standard-API budget. Measured sweep: 79s.

### Fixed — `mwpl.py` silent-zero paths

- **A futures contract the quotes API omits no longer reads as zero OI.**
  `fetch_futures_oi` returns the instrument keys that never came back and `sweep`
  reports them as an error instead of quietly understating that symbol.
- **Option strikes with OI but no delta are counted.** `safe_float(...) or 0.0`
  dropped them into the delta legs as zero while their OI still landed in
  `rawOI`; the shares now accumulate into `noDeltaOI`, surfaced as a dashboard
  banner and a CSV column. Currently 146,175 shares across NIFTYFPI, PFC,
  RECLTD.
- **`baseUtil` used a truthiness guard** — `if m and lim.get("nseFutEq")` turned a
  legitimate `0.0` into `None`. Now `is not None`.
- **`banned` could serialise as `null`** when the MWPL row was missing, since
  `util >= 95 or (None and ...)` evaluates to `None`. Wrapped in `bool()`.

### Added — dashboard guards in `static/mwpl.html`

- Banner when `FUTEQ_MODE` is not `"futures"`, when fewer than three option
  expiries are counted, and when any option OI arrived without a delta.
- `optDelta`, `optDeltaAbs` and `noDeltaOI` added to the CSV export; the meta
  line reports the fut-eq mode.

## 2026-09-08

### Fixed — `autologin1.py` verified against the live Upstox login service

Endpoint paths and payloads were read out of the `login.upstox.com` JS bundle and
confirmed end-to-end up to the PIN step:

- **OTP generate** must be `/login/open/v6/auth/1fa/otp/generate` with
  `{mobileNumber, userId, countryIsdCode}`. The `userId` comes from the `user_id`
  query param on the step-1 dialog redirect; omitting it returns the generic
  error 1017016.
- **OTP verify** is `/login/open/v5/auth/1fa/otp-totp/verify` (not `1fa/otp/verify`)
  with `{otp, validateOtpToken}`. The pyotp TOTP is accepted in place of the SMS OTP
  (`isTotpEnabled: true`), and the response carries `accounts[]` with `profileId`/`userId`.
- **PIN** is `/login/open/v3/auth/2fa` with `X-Profile-Id` / `X-User-Id` headers and
  `?client_id=&redirect_uri=` query, body `{twoFAMethod: "SECRET_PIN", inputText: PIN}`.
- Added `_check()` — the service answers HTTP 200 with `{"success": false, "error": {...}}`
  on failure, so status-code-only checks silently passed errors through.
- **The PIN must be base64-encoded.** The SPA sends `inputText: btoa(secretPin)`;
  a plaintext PIN comes back as "You have entered an incorrect PIN" (1017106), which
  reads like a wrong credential rather than a wrong encoding. The endpoint is
  `/v4/auth/2fa` (v3 rejects the payload) and the body also needs `profileId` as a number.
- **External OAuth apps need an explicit approval call.** 2FA succeeds with
  `redirectUri: null` and `isExternalClientOAuthApp: true`; the code only appears after
  `POST /login/v2/oauth/authorize?client_id=&response_type=code&redirect_uri=&requestId=`
  with `{"data": {"userOAuthApproval": true}}` (the SPA's `startOauth()`).
- A rejected PIN now aborts immediately: the endpoint locks the account after
  5 wrong attempts.
- `sys.stdout.reconfigure(encoding="utf-8")` at import — the status emoji crash
  `UnicodeEncodeError` on Windows whenever stdout is a pipe rather than a console.
- Rate limits worth knowing when debugging: OTP generate allows a few attempts then
  blocks for 10 minutes (1017069); the PIN allows 5 wrong attempts before lockout.

## 2026-09-07

### Added — `autologin1.py` (HTTP-only Upstox login)

- Mirrors `autologin.py` (token reuse -> refresh -> profile fetch, `.env` write-back)
  but replaces the headless-Playwright leg with direct `requests` calls to the
  endpoints the login page itself hits:
  OAuth dialog -> `1fa/otp/generate` -> `1fa/otp/verify` (pyotp TOTP) ->
  `2fa` (SECRET_PIN) -> `auth/redirect`, scraping `?code=` off whichever response
  carries it.
- Keeps a cookie-bearing `requests.Session` with browser-like headers
  (`x-device-details`, Origin/Referer on `login.upstox.com`).
- `AUTOLOGIN_DEBUG=1` dumps each request/response; the flow is wrapped in
  `main()` under `if __name__ == "__main__"` so it can be imported without
  triggering a login.

## 2026-08-11

### Added — MWPL utilisation dashboard (new app, port 8082)

- **`mwpl.py`** — standalone Flask app polling Upstox on a fixed cadence to compute
  live Market Wide Position Limit utilisation per F&O stock. Independent of
  `f2fspread.py` (port 8081); shares only `instruments.json` and
  `futurestockslist.csv`.
- **`static/mwpl.html`** — self-contained dashboard (no build step): KPI row,
  sortable/filterable table, per-row utilisation meter with ticks at the 80% and
  95% thresholds, CSV export, light/dark themes.
- **`combineoi.csv`** (supplied) — NSE's combined-OI report, read for the MWPL
  denominator per symbol plus a same-day baseline to measure drift against.

### Findings that shaped the implementation

- **NSE measures utilisation on delta-adjusted OI, not raw OI.** Reverse-engineered
  from `combineoi.csv` and confirmed on all 204 numeric rows to <1 share:
  `Limit for Next Day = 0.95 × MWPL − FutureEquivalentOI`.
  So `futEqOI = Σ(futures OI) + Σ(option OI × |delta|)` — which is why the option
  chain endpoint (it carries greeks) is the right source rather than plain quotes.
- **Ban has hysteresis.** A stock enters ban at ≥95% and stays banned until
  utilisation drops below 80%. Pinned down by SAIL and SAMMAANCAP, both flagged
  "No Fresh Positions" in the file at 92.7% / 90.7% — below the 95% entry trigger.
- **Upstox reports OI in shares, not lots**, for both futures and options (verified:
  every value divides by the contract lot size). No lot multiplication needed.
- **Futures OI batches.** All 208 near-month futures come from one
  `market-quote/quotes` call, not 208 — only the option chain needs one call per
  symbol. Cost is ~209 calls/sweep against a 2000/30min standard-API budget,
  putting the cadence floor near 190s; `POLL_INTERVAL_SEC` defaults to 240s.
  Measured sweep: 26s, 208 symbols, 0 errors.

### Known limitations

- **`EXPIRIES` is near-month only.** NSE counts every expiry; near-only ran at ~86%
  of true raw OI on RELIANCE (198.2M of 231.3M), so utilisation understates the
  official figure by roughly 10–15%. Widen the constant to
  `("current_month","next_month","far_month")` to match NSE, at 3× the call cost.
- **`DELTA_MODE` defaults to `"abs"`.** Absolute delta is the standard treatment
  (OI is gross exposure), but live data could not discriminate it from signed
  delta — both landed inside the ratio band observed in the file. A *current-dated*
  `combineoi.csv` would settle it by validating same-day live numbers against NSE's
  own future-equivalent column.
- **The supplied `combineoi.csv` is dated 09-MAR-2026 (155 days old).** MWPL values
  are revised periodically, and the drift already shows: LICI computes to 234%
  utilisation because its futures OI alone (59.7M) exceeds its March MWPL (33.2M) —
  the limit has since been raised. Ban counts are unreliable until the file is
  refreshed. The app warns on any file older than 45 days.
- **10 current F&O symbols have no MWPL row** (added since March: ADANIPOWER,
  COCHINSHIP, FORCEMOT, GODFRYPHLP, GVT&D, HYUNDAI, MOTILALOFS, NAM-INDIA, RADICO,
  VMM). They are surfaced as "no limit on file" rather than dropped.
