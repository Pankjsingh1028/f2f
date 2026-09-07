"""
MWPL Dashboard — Market Wide Position Limit utilisation, intraday
=================================================================
Polls Upstox on a fixed cadence and computes, per F&O stock, the live
delta-adjusted (future-equivalent) open interest against NSE's published MWPL.

NSE's own rule, reverse-engineered from combineoi.csv and confirmed on every
numeric row of that file to <1 share:

    Limit for Next Day = 0.95 * MWPL - FutureEquivalentOI

so utilisation is measured on FUTURE-EQUIVALENT (delta-weighted) OI, not raw OI.
A stock enters ban at >= 95% and — this is the part a naive reading misses —
stays banned until utilisation falls back below 80%. SAIL and SAMMAANCAP sit in
the sample file at 92.7% / 90.7% and are still flagged "No Fresh Positions",
which is what pins the hysteresis down.

    futEqOI = sum(near futures OI) + sum(option OI * |delta|)

OI from Upstox is already in SHARES for both futures and options (verified:
every value divides by the contract lot size), so no lot multiplication.

Cost per sweep: 1 batched market-quote call for all futures + 1 option/chain
call per symbol (~209 total). Upstox standard-API budget is 2000/30min, so the
floor is ~190s; POLL_INTERVAL_SEC defaults to 240s for headroom.

Run:
  1. Download the latest combineoi.csv from NSE into the repo root
  2. python mwpl.py                       # -> http://localhost:8082
"""

import os, sys, json, time, threading, collections
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, date

import pandas as pd
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

# ── CONFIG ──
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV       = "futurestockslist.csv"
COMBINEOI_CSV    = "combineoi.csv"
OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"
MARKET_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

# Expiries folded into the OI total. NSE counts every expiry; near-only runs at
# roughly 85% of the true figure (RELIANCE: 198.2M of 231.3M raw OI), so this
# understates utilisation. Widen to ("current_month","next_month","far_month")
# to match NSE exactly — cost is one extra API call per symbol per expiry.
EXPIRIES = ("current_month",)

# "abs"    — options contribute |delta| (gross exposure). NSE's standard.
# "signed" — puts net against calls. Kept switchable; abs is the default.
DELTA_MODE = "abs"

POLL_INTERVAL_SEC = 240      # >= ~190s to stay inside 2000 req / 30 min
REQ_PER_SEC       = 8        # per-second throttle (API cap is 50/s)
WORKERS           = 8
BAN_ENTER_PCT     = 95.0
BAN_EXIT_PCT      = 80.0

HDRS = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}

state = {"rows": [], "ts": None, "sweep_ms": None, "errors": [], "sweeps": 0}
state_lock = threading.Lock()


def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class RateLimiter:
    """Evenly spaced token bucket. Spacing rather than bursting keeps us clear
    of the per-second cap even when every worker wakes at once."""

    def __init__(self, per_sec):
        self.gap = 1.0 / per_sec
        self.lock = threading.Lock()
        self.next_at = 0.0

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = max(0.0, self.next_at - now)
            self.next_at = max(now, self.next_at) + self.gap
        if wait:
            time.sleep(wait)


limiter = RateLimiter(REQ_PER_SEC)


# ── STATIC DATA ──
def load_mwpl():
    """combineoi.csv -> {symbol: {mwpl, nseOI, nseFutEq, banned, date}}.

    The file is NSE's own snapshot, so it doubles as a same-day baseline: the
    dashboard shows how far live OI has moved since NSE last published."""
    if not os.path.exists(COMBINEOI_CSV):
        print(f"[warn] {COMBINEOI_CSV} not found — utilisation disabled")
        return {}, None
    df = pd.read_csv(COMBINEOI_CSV, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    out, file_date = {}, None
    for _, r in df.iterrows():
        sym = str(r["NSE Symbol"]).strip()
        limit_raw = str(r["Limit for Next Day"]).strip()
        file_date = file_date or str(r["Date"]).strip()
        out[sym] = {
            "mwpl": safe_float(r["MWPL"]),
            "nseOI": safe_float(r["Open Interest"]),
            "nseFutEq": safe_float(r["Future Equivalent Open Interest"]),
            # A non-numeric limit is NSE's way of saying the scrip is in ban.
            "banned": not limit_raw.replace(".", "").isdigit(),
        }
    return out, file_date


def load_universe():
    """Near-expiry futures key + option underlying key per F&O stock."""
    with open(INSTRUMENTS_JSON, "r") as f:
        instruments = json.load(f)
    syms = set(pd.read_csv(STOCKS_CSV)["underlying_symbol"].dropna())
    futs, ukey = collections.defaultdict(list), {}
    for x in instruments:
        if x.get("segment") != "NSE_FO":
            continue
        s = x.get("underlying_symbol")
        if s not in syms:
            continue
        if x.get("instrument_type") == "FUT":
            futs[s].append((x["expiry"], x["instrument_key"]))
        elif x.get("instrument_type") in ("CE", "PE"):
            ukey.setdefault(s, x.get("underlying_key"))
    uni = {}
    for s in sorted(syms):
        if s in futs and s in ukey:
            uni[s] = {"fut": min(futs[s])[1], "ukey": ukey[s]}
    return uni


# ── FETCH ──
def fetch_futures_oi(uni):
    """All near futures in one batched call (208 keys, well under the ~490 cap).
    -> {symbol: oi}"""
    by_key = {v["fut"]: s for s, v in uni.items()}
    keys = list(by_key)
    out = {}
    for i in range(0, len(keys), 490):
        batch = keys[i:i + 490]
        limiter.acquire()
        r = requests.get(MARKET_QUOTE_URL, params={"instrument_key": ",".join(batch)},
                         headers=HDRS, timeout=20)
        r.raise_for_status()
        for _, q in r.json().get("data", {}).items():
            # The response is keyed by trading symbol, not the instrument key we
            # sent, so map back through the token embedded in instrument_token.
            ik = q.get("instrument_token")
            if ik in by_key:
                out[by_key[ik]] = safe_float(q.get("oi")) or 0.0
    return out


def fetch_option_oi(sym, ukey):
    """Sum OI and delta-weighted OI across every strike of the configured
    expiries. -> (rawOI, futEqOI, spot, callOI, putOI)"""
    raw = fq = call_oi = put_oi = 0.0
    spot = None
    for kw in EXPIRIES:
        limiter.acquire()
        r = requests.get(OPTION_CHAIN_URL,
                         params={"instrument_key": ukey, "expiry_date": kw},
                         headers=HDRS, timeout=20)
        r.raise_for_status()
        for row in r.json().get("data", []) or []:
            spot = spot or safe_float(row.get("underlying_spot_price"))
            for side in ("call_options", "put_options"):
                o = row.get(side) or {}
                oi = safe_float((o.get("market_data") or {}).get("oi")) or 0.0
                dl = safe_float((o.get("option_greeks") or {}).get("delta")) or 0.0
                raw += oi
                fq += oi * (abs(dl) if DELTA_MODE == "abs" else dl)
                if side == "call_options":
                    call_oi += oi
                else:
                    put_oi += oi
    return raw, fq, spot, call_oi, put_oi


# ── SWEEP ──
def sweep():
    t0 = time.time()
    errors = []
    try:
        fut_oi = fetch_futures_oi(universe)
    except Exception as e:
        fut_oi = {}
        errors.append(f"futures: {e}")

    def one(sym):
        try:
            raw, fq, spot, c, p = fetch_option_oi(sym, universe[sym]["ukey"])
            return sym, raw, fq, spot, c, p, None
        except Exception as e:
            return sym, 0.0, 0.0, None, 0.0, 0.0, str(e)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(one, universe))

    rows = []
    for sym, raw, fq, spot, c, p, err in results:
        if err:
            errors.append(f"{sym}: {err}")
        f_oi = fut_oi.get(sym, 0.0)
        raw_total = raw + f_oi
        futeq = fq + f_oi                     # futures carry delta 1 by definition
        lim = mwpl_map.get(sym) or {}
        m = lim.get("mwpl")
        util = round(futeq / m * 100, 2) if m else None
        base = round(lim["nseFutEq"] / m * 100, 2) if m and lim.get("nseFutEq") else None
        rows.append({
            "sym": sym,
            "mwpl": m,
            "futOI": round(f_oi),
            "optOI": round(raw),
            "rawOI": round(raw_total),
            "futEq": round(futeq),
            "util": util,
            "baseUtil": base,
            "drift": round(util - base, 2) if (util is not None and base is not None) else None,
            # Shares of fresh future-equivalent exposure before the 95% trip.
            "headroom": round(0.95 * m - futeq) if m else None,
            "pcr": round(p / c, 3) if c else None,
            "spot": spot,
            # Live crossing vs NSE's last published ban state. Hysteresis means a
            # name already in ban stays in ban until it comes back under 80%.
            "banned": bool(lim.get("banned")) if util is None else (
                util >= BAN_ENTER_PCT or (lim.get("banned") and util >= BAN_EXIT_PCT)),
            "nseBanned": bool(lim.get("banned")),
            "noLimit": m is None,
        })
    rows.sort(key=lambda r: (r["util"] is None, -(r["util"] or 0)))
    with state_lock:
        state.update(rows=rows, ts=datetime.now().strftime("%H:%M:%S"),
                     sweep_ms=round((time.time() - t0) * 1000),
                     errors=errors[:20], sweeps=state["sweeps"] + 1)
    n_ban = sum(1 for r in rows if r["banned"])
    print(f"[{datetime.now():%H:%M:%S}] sweep {state['sweeps']} — {len(rows)} symbols, "
          f"{n_ban} in ban, {len(errors)} errors, {state['sweep_ms']}ms")


def poll_loop():
    while True:
        try:
            sweep()
        except Exception as e:
            print(f"[sweep failed] {e}")
        time.sleep(POLL_INTERVAL_SEC)


# ── INIT ──
print(f"[{datetime.now()}] Loading...")
mwpl_map, mwpl_date = load_mwpl()
universe = load_universe()
_missing = sorted(s for s in universe if s not in mwpl_map)
print(f"[{datetime.now()}] {len(universe)} symbols | MWPL file {mwpl_date} "
      f"({len(mwpl_map)} rows) | {len(_missing)} without a limit: {_missing}")

_stale = None
if mwpl_date:
    try:
        d = datetime.strptime(mwpl_date, "%d-%b-%Y").date()
        age = (date.today() - d).days
        if age > 45:
            _stale = f"combineoi.csv is dated {mwpl_date} ({age} days old) — MWPL values are revised periodically; re-download for accurate utilisation."
            print(f"[warn] {_stale}")
    except ValueError:
        pass

# ── FLASK ──
app = Flask(__name__, static_folder="static")


@app.route("/")
def index():
    return send_from_directory("static", "mwpl.html")


@app.route("/api/mwpl")
def api_mwpl():
    with state_lock:
        s = dict(state)
    return jsonify({
        **s,
        "mwplDate": mwpl_date,
        "stale": _stale,
        "expiries": list(EXPIRIES),
        "deltaMode": DELTA_MODE,
        "intervalSec": POLL_INTERVAL_SEC,
        "banEnter": BAN_ENTER_PCT,
        "banExit": BAN_EXIT_PCT,
    })


if __name__ == "__main__":
    threading.Thread(target=poll_loop, daemon=True).start()
    print(f"[{datetime.now()}] MWPL dashboard → http://localhost:8082")
    app.run(host="0.0.0.0", port=8082, debug=False, threaded=True)
