"""
Futures Spread Dashboard — Flask API Backend
==============================================
Serves JSON at /api/spreads for React frontend.
Serves React at / from ./static/index.html.

Run:
  1. python fsl.py                       # → futurestockslist.csv
  2. python f2fcalc.py                   # → margin_charges_cache.csv
  3. cd frontend && npm run build        # → static/app.js (only after UI edits)
  4. python f2fspread.py                 # → http://localhost:8081
"""

import os, sys, json, time, threading, bisect, collections
import pandas as pd
import requests
from datetime import datetime, timezone, date
from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory, request
import upstox_client

# Force UTF-8 console output so non-ASCII prints (→, ─, …) don't crash on
# Windows' default cp1252 stdout. Safe no-op on platforms already using UTF-8.
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
STOCKS_CSV = "futurestockslist.csv"
MARGIN_CSV = "margin_charges_cache.csv"
SPREADS_CSV = "future_spreads.csv"          # historical near/next/far spreads
MARKET_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

market_state = {}
market_state_lock = threading.Lock()
_state_dirty = True
_last_cache = None

def safe_float(x):
    try: return float(x)
    except: return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

def pctile(sorted_vals, x):
    """Percentile rank of x within a pre-sorted list (0-100, integer).
    e.g. 95 → today's spread is wider than 95% of its historical observations."""
    if not sorted_vals or x is None:
        return None
    return round(bisect.bisect_right(sorted_vals, x) / len(sorted_vals) * 100)

# ── DATA LOADING ──
def load_instruments():
    with open(INSTRUMENTS_JSON, "r") as f: return json.load(f)

def build_futures_index(instruments):
    raw, lots = {}, {}
    for inst in instruments:
        if inst.get("segment") == "NSE_FO" and inst.get("instrument_type") == "FUT":
            sym = inst.get("underlying_symbol")
            if sym:
                raw.setdefault(sym, []).append({"instrument_key": inst["instrument_key"], "expiry": inst.get("expiry", 0), "trading_symbol": inst.get("trading_symbol")})
                lots[inst["instrument_key"]] = inst.get("lot_size")
    idx = {}
    for sym, futs in raw.items():
        futs.sort(key=lambda x: x["expiry"])
        idx[sym] = futs[:3]
    return idx, lots

def load_underlyings():
    return pd.read_csv(STOCKS_CSV)["underlying_symbol"].dropna().unique().tolist()

def _clean(v):
    return None if (v is None or pd.isna(v)) else str(v)

def load_spread_history():
    """future_spreads.csv → {symbol: [ {date, nearExp, nextExp, farExp,
       nearClose, nextClose, farClose, nx, xf, nf}, ... ] } sorted by date."""
    if not os.path.exists(SPREADS_CSV):
        print(f"[warn] {SPREADS_CSV} not found — spread history disabled")
        return {}
    df = pd.read_csv(SPREADS_CSV, dtype={
        "Trade Date": str, "Near Expiry": str, "Next Expiry": str, "Far Expiry": str})
    hist = {}
    for sym, g in df.groupby("Symbol", sort=False):
        g = g.sort_values("Trade Date")
        hist[sym] = [{
            "date": r["Trade Date"],
            "nearExp": _clean(r["Near Expiry"]), "nextExp": _clean(r["Next Expiry"]),
            "farExp": _clean(r["Far Expiry"]),
            "nearClose": safe_float(r["Near Close"]), "nextClose": safe_float(r["Next Close"]),
            "farClose": safe_float(r["Far Close"]),
            "nx": safe_float(r["Near-Next Spread"]), "xf": safe_float(r["Next-Far Spread"]),
            "nf": safe_float(r["Near-Far Spread"]),
        } for _, r in g.iterrows()]
    print(f"[{datetime.now()}] Spread history: {len(hist)} symbols from {SPREADS_CSV}")
    return hist

def annotate_cycle_days(hist):
    """Tag every history row with its cycle day — day 1 being the first session
    after the previous expiry, the same axis the spread chart plots on. Each
    symbol's opening cycle gets None: its roll happened before the file starts,
    so those day numbers would be shifted and must not enter an average."""
    for rows in hist.values():
        start, prev_exp, anchored = None, None, False
        for r in rows:
            if r["nearExp"] != prev_exp:
                anchored = prev_exp is not None
                start, prev_exp = r["date"], r["nearExp"]
            r["cday"] = ((date.fromisoformat(r["date"]) - date.fromisoformat(start)).days + 1
                         if anchored else None)

def build_spread_dist(hist):
    """Pre-sort each symbol's historical nx/xf/nf spreads so a live percentile
    lookup is a cheap bisect. → {symbol: {nx: [...], xf: [...], nf: [...]}}."""
    dist = {}
    for sym, rows in hist.items():
        dist[sym] = {k: sorted(r[k] for r in rows if r.get(k) is not None)
                     for k in ("nx", "xf", "nf")}
    return dist

# ── HISTORICAL WINDOW STATS (Avg / High / Low per spread pair) ──
# Lookback key → calendar months back from today; None = every row on file.
LOOKBACK_MONTHS = {"1m": 1, "3m": 3, "6m": 6, "1y": 12, "all": None}
DEFAULT_LOOKBACK = "6m"
_stats_cache = {}                                  # (lookback, today) → {sym: {...}}

def window_bounds(lookback):
    """(start, end) ISO dates for a lookback key. `start` is inclusive (None for
    "all"); `end` is EXCLUSIVE and is always today, so the live session can never
    feed into the benchmark it is being compared against."""
    end = datetime.now().strftime("%Y-%m-%d")
    months = LOOKBACK_MONTHS.get(lookback)
    if months is None:
        return None, end
    return (pd.Timestamp(end) - pd.DateOffset(months=months)).strftime("%Y-%m-%d"), end

def compute_hist_stats(lookback):
    """Per symbol, over the lookback window: average / high / low of each pair's
    close spread, plus AL/ALD — the lowest the per-day average gets, and the
    cycle day it falls on. Dates are ISO so plain string compares order them
    correctly. → {symbol: {aNX,hNX,lNX,alNX,aldNX, aXF,...}}."""
    start, end = window_bounds(lookback)
    out = {}
    for sym, rows in spread_history.items():
        win = [r for r in rows
               if r["date"] < end and (start is None or r["date"] >= start)]
        # A cycle day only qualifies for ALD if it shows up in at least half the
        # window's cycles. Late days (30+) exist in only the odd long cycle, and
        # without this one stray reading could win the minimum on a sample of 1.
        n_cycles = len({r["nearExp"] for r in win if r["nearExp"]})
        min_obs = max(1, n_cycles // 2)
        st = {}
        for key, sfx in (("nx", "NX"), ("xf", "XF"), ("nf", "NF")):
            vals, by_day = [], collections.defaultdict(list)
            for r in win:
                v = r[key]
                # A missing leg leaves NaN (not None) in the CSV — `v == v` drops
                # it, otherwise one NaN would swallow the whole avg/max/min.
                if v is None or v != v:
                    continue
                vals.append(v)
                if r["cday"] is not None:
                    by_day[r["cday"]].append(v)
            if vals:
                st["a" + sfx] = round(sum(vals) / len(vals), 2)
                st["h" + sfx] = round(max(vals), 2)
                st["l" + sfx] = round(min(vals), 2)
            else:
                st["a" + sfx] = st["h" + sfx] = st["l" + sfx] = None
            # Where the chart's historical-average line bottoms out: AL is that
            # low value, ALD the cycle day it lands on. The sort key is rounded
            # well past display precision first — summation order alone can put
            # two mathematically equal averages a float ULP apart, which would
            # otherwise decide the answer arbitrarily. Genuine gaps are far wider
            # than 1e-6, so real differences survive and true ties fall through to
            # the earlier day — stable run to run.
            days = [d for d, vs in by_day.items() if len(vs) >= min_obs]
            if days:
                best = min(days, key=lambda d: (round(sum(by_day[d]) / len(by_day[d]), 6), d))
                st["ald" + sfx] = best
                st["al" + sfx] = round(sum(by_day[best]) / len(by_day[best]), 2)
            else:
                st["ald" + sfx] = st["al" + sfx] = None
        out[sym] = st
    return out

def get_hist_stats(lookback):
    """Cached per (lookback, day) — the source CSV is static for a session, so a
    window only needs recomputing when the calendar date rolls over."""
    today = datetime.now().strftime("%Y-%m-%d")
    ck = (lookback, today)
    if ck not in _stats_cache:
        for stale in [k for k in _stats_cache if k[1] != today]:
            del _stats_cache[stale]                # drop windows built for an earlier date
        _stats_cache[ck] = compute_hist_stats(lookback)
    return _stats_cache[ck]

# ── EXPIRY CYCLE POSITION ──
# "Day of expiry" = how far the running cycle has come: day 1 is the first
# session after the previous expiry. Same definition the spread chart uses for
# its x-axis, so the header number and the chart's Today marker line up.
_cycle_cache = {}

def _expiry_date(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

def _near_expiry(sym=None):
    """ISO expiry of the near contract — for one symbol, or the one the market as
    a whole is trading (in practice every symbol shares it)."""
    if sym is not None:
        futs = futures_index.get(sym) or []
        return _expiry_date(futs[0]["expiry"]) if futs else None
    cnt = collections.Counter()
    for s in underlyings:
        futs = futures_index.get(s) or []
        if futs:
            cnt[futs[0]["expiry"]] += 1
    return _expiry_date(cnt.most_common(1)[0][0]) if cnt else None

def _cycle_start(expiry, sym=None):
    """First session of the cycle ending on `expiry`, read off the history rather
    than assumed, so a symbol that skips a month still anchors correctly."""
    syms = [sym] if sym is not None else list(spread_history)
    starts, prev = collections.Counter(), ""
    for s in syms:
        for r in spread_history.get(s) or []:
            if r["nearExp"] == expiry:
                starts[r["date"]] += 1     # rows are date-sorted → first match is the start
                break
            if r["nearExp"] and prev < r["nearExp"] < expiry:
                prev = r["nearExp"]
    if starts:
        return starts.most_common(1)[0][0]
    # Roll happened after the CSV was last rebuilt, so the new cycle isn't on
    # file yet: fall back to the day after the previous expiry.
    return (pd.Timestamp(prev) + pd.Timedelta(days=1)).strftime("%Y-%m-%d") if prev else None

def cycle_info(sym=None):
    """{day, start, expiry, dte} for today. Cached per (symbol, date) — it only
    moves when the calendar does."""
    today = datetime.now().strftime("%Y-%m-%d")
    ck = (sym, today)
    if ck not in _cycle_cache:
        for stale in [k for k in _cycle_cache if k[1] != today]:
            del _cycle_cache[stale]
        exp = _near_expiry(sym)
        start = _cycle_start(exp, sym) if exp else None
        _cycle_cache[ck] = {
            "day": (pd.Timestamp(today) - pd.Timestamp(start)).days + 1 if start else None,
            "start": start, "expiry": exp,
            "dte": (pd.Timestamp(exp) - pd.Timestamp(today)).days if exp else None,
        }
    return _cycle_cache[ck]

def load_margin_data():
    if not os.path.exists(MARGIN_CSV): return {}
    df = pd.read_csv(MARGIN_CSV)
    d = {}
    for _, r in df.iterrows():
        s = r.get("Symbol")
        if pd.isna(s): continue
        lot = int(r["Lot_Size"]) if pd.notna(r.get("Lot_Size")) else None
        d[s] = {"Lot_Size": lot, "Margin": safe_float(r.get("Margin")), "Charges": safe_float(r.get("Charges")), "Cost_of_Carry": safe_float(r.get("Cost_of_Carry"))}
    return d

# ── REST POLL ──
def initial_rest_poll(keys):
    global _state_dirty
    print(f"[{datetime.now()}] REST poll: {len(keys)} instruments...")
    hdrs = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
    for i in range(0, len(keys), 490):
        batch = keys[i:i+490]
        try:
            resp = requests.get(f"{MARKET_QUOTE_URL}?instrument_key={','.join(batch)}", headers=hdrs, timeout=15)
            if resp.status_code == 401: print("FATAL: 401"); return
            resp.raise_for_status()
            with market_state_lock:
                for ik_l, q in resp.json().get("data", {}).items():
                    ik = q.get("instrument_token") or ik_l
                    d = q.get("depth", {}); bd = d.get("buy", []); sd = d.get("sell", [])
                    market_state[ik] = {"bidP": safe_float(bd[0].get("price")) if bd else None, "askP": safe_float(sd[0].get("price")) if sd else None, "ltp": safe_float(q.get("last_price"))}
                _state_dirty = True
            print(f"  Batch {i//490+1} OK")
        except Exception as e: print(f"  Batch FAILED: {e}")
        time.sleep(0.1)
    print(f"[{datetime.now()}] Poll done. {len(market_state)} instruments.")

# ── WEBSOCKET ──
def on_message(msg):
    global _state_dirty
    if msg.get("type") != "live_feed": return
    feeds = msg.get("feeds", {})
    if not feeds: return
    with market_state_lock:
        for ik, p in feeds.items():
            ff = p.get("fullFeed", {}).get("marketFF", {}); ltpc = ff.get("ltpc", {}); dl = ff.get("marketLevel", {}).get("bidAskQuote", [])
            market_state[ik] = {"bidP": safe_float(dl[0].get("bidP")) if dl else None, "askP": safe_float(dl[0].get("askP")) if dl else None, "ltp": safe_float(ltpc.get("ltp"))}
        _state_dirty = True

def start_websocket(keys):
    def _run():
        cfg = upstox_client.Configuration(); cfg.access_token = ACCESS_TOKEN
        s = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg), keys, "full")
        s.on("message", on_message); s.on("open", lambda: print(f"[{datetime.now()}] WS connected"))
        s.on("error", lambda e: print(f"[{datetime.now()}] WS error: {e}")); s.on("reconnecting", lambda: print(f"[{datetime.now()}] WS reconnecting"))
        s.auto_reconnect(True, 5, 5); s.connect()
        while True: time.sleep(1)
    threading.Thread(target=_run, daemon=True).start()

# ── SPREADS ──
def compute_spreads(n, x, f):
    # Each calendar spread is (later − earlier) and has ONE two-sided quote,
    # both from executable prices:
    #   Bid = sell the spread = later.bid − earlier.ask
    #   Ask = buy  the spread = later.ask − earlier.bid   (Ask ≥ Bid always)
    return {"sNX": diff(x.get("bidP"), n.get("askP")),  # Near-Next Bid
            "sXN": diff(x.get("askP"), n.get("bidP")),  # Near-Next Ask
            "sXF": diff(f.get("bidP"), x.get("askP")),  # Next-Far  Bid
            "sFX": diff(f.get("askP"), x.get("bidP")),  # Next-Far  Ask
            "sNF": diff(f.get("bidP"), n.get("askP")),  # Near-Far  Bid
            "sFN": diff(f.get("askP"), n.get("bidP"))}  # Near-Far  Ask

def build_records(snap):
    e = {"ltp": None, "bidP": None, "askP": None}; rows = []
    for sym in underlyings:
        futs = futures_index.get(sym, []); near = futs[0] if len(futs)>0 else None; nxt = futs[1] if len(futs)>1 else None; far = futs[2] if len(futs)>2 else None
        ns = snap.get(near["instrument_key"], e) if near else e; xs = snap.get(nxt["instrument_key"], e) if nxt else e; fs = snap.get(far["instrument_key"], e) if far else e
        sp = compute_spreads(ns, xs, fs); m = margin_dict.get(sym, {}); lot = m.get("Lot_Size"); mg = m.get("Margin"); ch = m.get("Charges"); co = m.get("Cost_of_Carry")
        nLtp, xLtp, fLtp = ns.get("ltp"), xs.get("ltp"), fs.get("ltp")
        # LTP spread = later.ltp − earlier.ltp (the mid-ish reference for each pair)
        lsNX, lsXF, lsNF = diff(xLtp, nLtp), diff(fLtp, xLtp), diff(fLtp, nLtp)
        # r* = LTP spread as % of the near LTP. No longer a table column — kept in
        # the payload because the dashboard's "|%| ≥" row filter runs off it.
        nl = nLtp or 1
        def rpct(v): return round((v / nl) * 100, 2) if v is not None else None
        # LTP %ile = rank of the LTP spread within that pair's historical close spread.
        dd = spread_dist.get(sym, {})
        derived = {"lsNX": lsNX, "rNX": rpct(lsNX), "pNX": pctile(dd.get("nx"), lsNX),
                   "lsXF": lsXF, "rXF": rpct(lsXF), "pXF": pctile(dd.get("xf"), lsXF),
                   "lsNF": lsNF, "rNF": rpct(lsNF), "pNF": pctile(dd.get("nf"), lsNF)}
        rows.append({"sym": sym, "lot": lot, "mgn": round(mg,2) if mg else None, "chg": round(ch/lot,2) if (ch and lot) else None, "coc": round(co/lot,2) if (co and lot) else None, "nLtp": nLtp, "xLtp": xLtp, "fLtp": fLtp, **sp, **derived})
    return rows

# ── INIT ──
print(f"[{datetime.now()}] Loading...")
_raw = load_instruments(); underlyings = load_underlyings(); margin_dict = load_margin_data()
spread_history = load_spread_history()
annotate_cycle_days(spread_history)
spread_dist = build_spread_dist(spread_history)
futures_index, lot_sizes = build_futures_index(_raw); del _raw
subscribe_keys = list(dict.fromkeys([f["instrument_key"] for s in underlyings for f in futures_index.get(s, [])]))
print(f"[{datetime.now()}] {len(underlyings)} symbols, {len(subscribe_keys)} contracts")

# ── FLASK ──
app = Flask(__name__, static_folder="static")

@app.route("/")
def index(): return send_from_directory("static", "index.html")

@app.route("/api/history/<symbol>")
def api_history(symbol):
    sym = symbol if symbol in spread_history else symbol.upper()
    rows = spread_history.get(sym) or []
    return jsonify({"symbol": symbol, "rows": rows, "cycle": cycle_info(sym)})

@app.route("/api/spread-stats")
def api_spread_stats():
    """Avg / high / low of each pair's spread over ?lookback= (1m|3m|6m|1y|all),
    always excluding today. Static for the session, so the UI fetches it once per
    lookback change rather than on the 500 ms live poll."""
    lb = request.args.get("lookback", DEFAULT_LOOKBACK)
    if lb not in LOOKBACK_MONTHS:
        lb = DEFAULT_LOOKBACK
    start, end = window_bounds(lb)
    return jsonify({"lookback": lb, "from": start, "toExclusive": end,
                    "data": get_hist_stats(lb)})

@app.route("/api/spreads")
def api_spreads():
    global _state_dirty, _last_cache
    if not _state_dirty and _last_cache is not None:
        return jsonify({"data": _last_cache, "ts": datetime.now().strftime("%H:%M:%S"), "live": len(market_state), "cycle": cycle_info(), "cached": True})
    with market_state_lock:
        snap = dict(market_state); _state_dirty = False
    _last_cache = build_records(snap)
    return jsonify({"data": _last_cache, "ts": datetime.now().strftime("%H:%M:%S"), "live": len(snap), "cycle": cycle_info(), "cached": False})

if __name__ == "__main__":
    initial_rest_poll(subscribe_keys); start_websocket(subscribe_keys)
    os.makedirs("static", exist_ok=True)
    print(f"[{datetime.now()}] Dashboard → http://localhost:8081")
    app.run(host="0.0.0.0", port=8081, debug=False, threaded=True)
