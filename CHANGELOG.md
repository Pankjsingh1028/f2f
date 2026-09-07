# Changelog

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
