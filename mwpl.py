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

Despite the column's name, that published figure tracks the FUTURES open
interest, not a delta-weighted total. Measured live against the 10-SEP-2026
file across all 208 comparable symbols, with each symbol's legs scaled back by
its own one-session drift (our total OI / NSE's total OI, median 1.0036):

    NSE futEq / futures OI      median 1.0345   p10 0.9963   p90 1.0902

    formula                      bias   |err| med   |err| p90   within 5pp
    futures only                -0.99        0.95        2.75         98%
    futures + signed options    -2.23        2.05        6.65         84%
    futures + abs options       +6.84        5.58       13.72         45%

Errors are percentage points of MWPL. Adding a delta-weighted option leg in
either sign convention makes the match WORSE, so this module measures
utilisation on the futures leg by default (FUTEQ_MODE).

The option book is not literally ignored by NSE - the implied contribution is
about +5% of option OI (p10 -0.3%, p90 +10.2%) - but that is far below what
either convention produces: our own chains weigh in at -7.8% of option OI
signed and +33.8% absolute. The exact treatment is not recoverable from this
file, and a fitted constant (futures + 0.0339 * optionOI, |err| med 0.54) buys
0.4pp from a free parameter, so it is documented here rather than shipped.

Absolute delta, the original default, overstated every one of the 208 symbols
and is what made IREDA read 95% against NSE's 81.7%.

Every expiry counts, futures as well as options - for IREDA the far months
hold 17% of futures OI and 8% of option OI.

OI from Upstox is already in SHARES for both futures and options (verified:
every value divides by the contract lot size), so no lot multiplication.

Cost per sweep: 2 batched market-quote calls for all futures (~633 keys) + 1
option/chain call per symbol per expiry (~633 total). Upstox standard-API
budget is 2000/30min, so the floor is ~570s; POLL_INTERVAL_SEC defaults to 600s.
Both option legs come out of the same pass, so FUTEQ_MODE is free to change.

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

# Expiries folded into the OI total, for futures as well as options. NSE counts
# every one. Narrowing this saves an option/chain call per symbol per expiry
# dropped, at the cost of understating utilisation.
EXPIRIES = ("current_month", "next_month", "far_month")

# Which legs build the future-equivalent OI that utilisation is measured on.
# See the module docstring for the 208-symbol reconciliation behind the default.
#
# "futures" — futures OI alone. Reproduces NSE's published column to ~1pp of
#             MWPL (98% of symbols within 5pp) and is the only mode that agrees
#             with every one of NSE's five ban flags without a false positive.
# "signed"  — futures + sum(option OI * delta), puts netting against calls.
# "abs"     — futures + sum(option OI * |delta|). Overstates every symbol.
#
# Both option legs are fetched and reported either way (optDelta / optDeltaAbs),
# so this switch only decides which one feeds utilisation.
FUTEQ_MODE = "futures"

POLL_INTERVAL_SEC = 600      # >= ~570s at 3 expiries to stay inside 2000/30min
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
            # Every futures expiry, nearest first. Narrowing EXPIRIES trims the
            # option legs only; NSE counts all futures months regardless.
            uni[s] = {"futs": [k for _, k in sorted(futs[s])], "ukey": ukey[s]}
    return uni


# ── FETCH ──
def fetch_futures_oi(uni):
    """Every futures expiry, batched 490 keys at a time (~627 keys total).
    -> ({symbol: summed oi}, [instrument keys the API never returned])"""
    by_key = {k: s for s, v in uni.items() for k in v["futs"]}
    keys = list(by_key)
    out, seen = collections.defaultdict(float), set()
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
                out[by_key[ik]] += safe_float(q.get("oi")) or 0.0
                seen.add(ik)
    # A contract the API skipped would otherwise read as a genuine zero and
    # silently understate utilisation, so hand the gap back to the caller.
    return dict(out), [k for k in keys if k not in seen]


def fetch_option_oi(sym, ukey):
    """Sum OI and BOTH delta-weighted option legs across every strike of the
    configured expiries. One pass yields both conventions, so FUTEQ_MODE can
    change without costing another call.
    -> (rawOI, signedDeltaOI, absDeltaOI, spot, callOI, putOI, oiMissingDelta)"""
    raw = sgn = ab = call_oi = put_oi = nodelta = 0.0
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
                dl = safe_float((o.get("option_greeks") or {}).get("delta"))
                if not dl:
                    # Drops out of futEq while its OI still lands in rawOI -
                    # track the shares so the gap is visible, not silent.
                    nodelta += oi
                    dl = 0.0
                raw += oi
                sgn += oi * dl
                ab += oi * abs(dl)
                if side == "call_options":
                    call_oi += oi
                else:
                    put_oi += oi
    return raw, sgn, ab, spot, call_oi, put_oi, nodelta


# ── SWEEP ──
def sweep():
    t0 = time.time()
    errors = []
    try:
        fut_oi, fut_missing = fetch_futures_oi(universe)
        if fut_missing:
            errors.append(f"futures: no quote for {len(fut_missing)} contract(s) "
                          f"({', '.join(fut_missing[:5])}) — counted as 0 OI")
    except Exception as e:
        fut_oi = {}
        errors.append(f"futures: {e}")

    def one(sym):
        try:
            raw, sgn, ab, spot, c, p, nd = fetch_option_oi(sym, universe[sym]["ukey"])
            return sym, raw, sgn, ab, spot, c, p, nd, None
        except Exception as e:
            return sym, 0.0, 0.0, 0.0, None, 0.0, 0.0, 0.0, str(e)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(one, universe))

    rows = []
    for sym, raw, sgn, ab, spot, c, p, nd, err in results:
        if err:
            errors.append(f"{sym}: {err}")
        f_oi = fut_oi.get(sym, 0.0)
        raw_total = raw + f_oi
        # Futures carry delta 1 by definition; the option leg is whatever
        # FUTEQ_MODE selects, and "futures" is NSE's own published basis.
        futeq = f_oi + {"signed": sgn, "abs": ab}.get(FUTEQ_MODE, 0.0)
        lim = mwpl_map.get(sym) or {}
        m = lim.get("mwpl")
        nse_fq = lim.get("nseFutEq")
        util = round(futeq / m * 100, 2) if m else None
        base = round(nse_fq / m * 100, 2) if m and nse_fq is not None else None
        rows.append({
            "sym": sym,
            "mwpl": m,
            "futOI": round(f_oi),
            "optOI": round(raw),
            "rawOI": round(raw_total),
            "futEq": round(futeq),
            # Both option legs, reported regardless of which one feeds futEq.
            "optDelta": round(sgn),
            "optDeltaAbs": round(ab),
            "util": util,
            "baseUtil": base,
            "drift": round(util - base, 2) if (util is not None and base is not None) else None,
            # Shares of fresh future-equivalent exposure before the 95% trip.
            "headroom": round(0.95 * m - futeq) if m else None,
            "pcr": round(p / c, 3) if c else None,
            # Option OI the greeks endpoint gave no delta for: in rawOI, not futEq.
            "noDeltaOI": round(nd),
            "spot": spot,
            # Live crossing vs NSE's last published ban state. Hysteresis means a
            # name already in ban stays in ban until it comes back under 80%.
            "banned": bool(lim.get("banned")) if util is None else bool(
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
        "futEqMode": FUTEQ_MODE,
        "intervalSec": POLL_INTERVAL_SEC,
        "banEnter": BAN_ENTER_PCT,
        "banExit": BAN_EXIT_PCT,
    })


if __name__ == "__main__":
    threading.Thread(target=poll_loop, daemon=True).start()
    print(f"[{datetime.now()}] MWPL dashboard → http://localhost:8082")
    app.run(host="0.0.0.0", port=8082, debug=False, threaded=True)
