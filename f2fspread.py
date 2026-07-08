"""
Futures Spread Dashboard — Flask API Backend
==============================================
Serves JSON at /api/spreads for React frontend.
Serves React at / from ./static/index.html.

Run:
  1. python futurestockslist.py
  2. python f2fprecalc_optimized.py
  3. python f2fspread_final.py  →  http://localhost:8051
"""

import os, json, time, threading
import pandas as pd
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify, send_from_directory
import upstox_client

# ── CONFIG ──
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
MARGIN_CSV = "margin_charges_cache.csv"
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
    nl = safe_float(n.get("ltp")) or 1
    def pct(v): return round((v / nl) * 100, 2) if v is not None else None
    ab = {"sNX": diff(x.get("bidP"), n.get("askP")), "sXN": diff(n.get("bidP"), x.get("askP")), "sXF": diff(f.get("bidP"), x.get("askP")), "sFX": diff(x.get("bidP"), f.get("askP")), "sNF": diff(f.get("bidP"), n.get("askP")), "sFN": diff(n.get("bidP"), f.get("askP"))}
    return {**ab, **{k+"p": pct(v) for k, v in ab.items()}}

def build_records(snap):
    e = {"ltp": None, "bidP": None, "askP": None}; rows = []
    for sym in underlyings:
        futs = futures_index.get(sym, []); near = futs[0] if len(futs)>0 else None; nxt = futs[1] if len(futs)>1 else None; far = futs[2] if len(futs)>2 else None
        ns = snap.get(near["instrument_key"], e) if near else e; xs = snap.get(nxt["instrument_key"], e) if nxt else e; fs = snap.get(far["instrument_key"], e) if far else e
        sp = compute_spreads(ns, xs, fs); m = margin_dict.get(sym, {}); lot = m.get("Lot_Size"); mg = m.get("Margin"); ch = m.get("Charges"); co = m.get("Cost_of_Carry")
        rows.append({"sym": sym, "lot": lot, "mgn": round(mg,2) if mg else None, "chg": round(ch/lot,2) if (ch and lot) else None, "coc": round(co/lot,2) if (co and lot) else None, "nLtp": ns.get("ltp"), "xLtp": xs.get("ltp"), "fLtp": fs.get("ltp"), **sp})
    return rows

# ── INIT ──
print(f"[{datetime.now()}] Loading...")
_raw = load_instruments(); underlyings = load_underlyings(); margin_dict = load_margin_data()
futures_index, lot_sizes = build_futures_index(_raw); del _raw
subscribe_keys = list(dict.fromkeys([f["instrument_key"] for s in underlyings for f in futures_index.get(s, [])]))
print(f"[{datetime.now()}] {len(underlyings)} symbols, {len(subscribe_keys)} contracts")

# ── FLASK ──
app = Flask(__name__, static_folder="static")

@app.route("/")
def index(): return send_from_directory("static", "index.html")

@app.route("/api/spreads")
def api_spreads():
    global _state_dirty, _last_cache
    if not _state_dirty and _last_cache is not None:
        return jsonify({"data": _last_cache, "ts": datetime.now().strftime("%H:%M:%S"), "live": len(market_state), "cached": True})
    with market_state_lock:
        snap = dict(market_state); _state_dirty = False
    _last_cache = build_records(snap)
    return jsonify({"data": _last_cache, "ts": datetime.now().strftime("%H:%M:%S"), "live": len(snap), "cached": False})

if __name__ == "__main__":
    initial_rest_poll(subscribe_keys); start_websocket(subscribe_keys)
    os.makedirs("static", exist_ok=True)
    print(f"[{datetime.now()}] Dashboard → http://localhost:8081")
    app.run(host="0.0.0.0", port=8081, debug=False, threaded=True)
