import os
import json
import time
import threading
import pandas as pd
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output

import upstox_client
from upstox_client.rest import ApiException

# ---------- CONFIG ----------
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
MARGIN_CSV = "margin_charges_cache.csv"
REFRESH_INTERVAL = 2500  # ms
MARKET_QUOTE_URL = 'https://api.upstox.com/v2/market-quote/quotes'

market_state = {}
#market_state_lock = threading.Lock()
import eventlet
market_state_lock = eventlet.semaphore.Semaphore()

# ---------- HELPERS ----------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

def load_instruments():
    try:
        with open(INSTRUMENTS_JSON, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {INSTRUMENTS_JSON} not found.")
        return []


def load_underlyings():
    try:
        df = pd.read_csv(STOCKS_CSV)
        return df["underlying_symbol"].dropna().unique().tolist()
    except FileNotFoundError:
        print(f"Error: {STOCKS_CSV} not found.")
        return []

def get_instrument_keys_for_symbol(symbol, instruments):
    futures = []
    for instrument in instruments:
        if (
            instrument.get("segment") == "NSE_FO"
            and instrument.get("instrument_type") == "FUT"
            and instrument.get("underlying_symbol") == symbol
        ):
            futures.append({
                "instrument_key": instrument.get("instrument_key"),
                "expiry": instrument.get("expiry", 0),
                "trading_symbol": instrument.get("trading_symbol")
            })
    futures.sort(key=lambda x: x["expiry"])
    return futures[:3]

# ---------- REST POLLING ----------
def initial_rest_poll(subscribe_keys):
    print(f"[{datetime.now()}] Performing initial REST poll for {len(subscribe_keys)} instruments...")
    batch_size = 490
    for i in range(0, len(subscribe_keys), batch_size):
        keys_batch = subscribe_keys[i:i + batch_size]
        instrument_keys_str = ",".join(keys_batch)
        url = f"{MARKET_QUOTE_URL}?instrument_key={instrument_keys_str}"
        headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                with market_state_lock:
                    for ik_long, quote in data.items():
                        depth = quote.get("depth", {})
                        buy_depth = depth.get("buy", [])
                        sell_depth = depth.get("sell", [])
                        bid = safe_float(buy_depth[0].get("price")) if buy_depth else None
                        ask = safe_float(sell_depth[0].get("price")) if sell_depth else None
                        ltp = safe_float(quote.get("last_price"))
                        ik = quote.get("instrument_token") or ik_long
                        market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}
                print(f"  ✅ Batch {i//batch_size + 1} ({len(keys_batch)} instruments)")
            else:
                print(f"[Poll] HTTP {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            print(f"[Poll] Error: {e}")
        time.sleep(0.1)
    print(f"[{datetime.now()}] Initial poll complete.")

# ---------- WEBSOCKET CALLBACKS ----------
def on_message(message):
    if message.get("type") != "live_feed":
        return
    feeds = message.get("feeds", {})
    for ik, payload in feeds.items():
        ff = payload.get("fullFeed", {}).get("marketFF", {})
        ltpc = ff.get("ltpc", {})
        ltp = safe_float(ltpc.get("ltp"))
        depth_list = ff.get("marketLevel", {}).get("bidAskQuote", [])
        bid = safe_float(depth_list[0].get("bidP")) if depth_list else None
        ask = safe_float(depth_list[0].get("askP")) if depth_list else None
        with market_state_lock:
            market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}

#    print(f"[WS] Updated {len(feeds)} feeds | total tracked: {len(market_state)}", flush=True) 

def on_open():
    print(f"[{datetime.now()}] SDK WebSocket connected")

def on_error(error):
    print(f"[{datetime.now()}] SDK Error: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] SDK Reconnecting...")

def start_sdk_streamer(subscribe_keys):
    def _run():
        configuration = upstox_client.Configuration()
        configuration.access_token = ACCESS_TOKEN
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client, subscribe_keys, "full")
        streamer.on("message", on_message)
        streamer.on("open", on_open)
        streamer.on("error", on_error)
        streamer.on("reconnecting", on_reconnecting)
        streamer.auto_reconnect(True, 5, 5)
        streamer.connect()
        while True:
            time.sleep(0.5)
    t = threading.Thread(target=_run, daemon=True)
    t.start()

# ---------- CORE DASHBOARD ----------
def compute_spreads(near, nxt, far):
    near_ltp = safe_float(near.get("ltp")) or 1
    def pct(a, b):
        d = diff(a, b)
        return round((d / near_ltp) * 100, 2) if d is not None else None

    abs_spreads = {
        "Spread_NearBuy_NextSell": diff(nxt.get("bidP"), near.get("askP")),
        "Spread_NearSell_NextBuy": diff(near.get("bidP"), nxt.get("askP")),
        "Spread_NextBuy_FarSell": diff(far.get("bidP"), nxt.get("askP")),
        "Spread_NextSell_FarBuy": diff(nxt.get("bidP"), far.get("askP")),
        "Spread_NearBuy_FarSell": diff(far.get("bidP"), near.get("askP")),
        "Spread_NearSell_FarBuy": diff(near.get("bidP"), far.get("askP")),
    }

    pct_spreads = {k + "_pct": pct(v, 0) for k, v in abs_spreads.items()}
    return {**abs_spreads, **pct_spreads}

def get_state(ik):
    with market_state_lock:
        return market_state.get(ik, {"ltp": None, "bidP": None, "askP": None})

def build_df(underlyings, instruments, margin_df):
    rows = []
    for sym in underlyings:
        futs = get_instrument_keys_for_symbol(sym, instruments)
        if len(futs) < 3:
            futs += [None] * (3 - len(futs))
        near, nxt, far = futs[:3]
        near_s = get_state(near["instrument_key"]) if near else {}
        nxt_s = get_state(nxt["instrument_key"]) if nxt else {}
        far_s = get_state(far["instrument_key"]) if far else {}

        spreads = compute_spreads(near_s, nxt_s, far_s)

        mrow = margin_df.loc[margin_df["Symbol"] == sym]
        if not mrow.empty:
            margin = float(mrow["Margin"].iloc[0])
            charges = float(mrow["Charges"].iloc[0])
            carry = float(mrow["Cost_of_Carry"].iloc[0])
            lot_size = int(mrow["Lot_Size"].iloc[0])
        else:
            margin = charges = carry = lot_size = None

        rows.append({
            "Symbol": sym,
            "Lot_Size": lot_size,
            "Margin": round(margin or 0, 2),
            "Charges": round((charges or 0) / (lot_size or 1), 2),
            "Cost_of_Carry": round((carry or 0) / (lot_size or 1), 2),
            "Near_ltp": near_s.get("ltp"),
            "Next_ltp": nxt_s.get("ltp"),
            "Far_ltp": far_s.get("ltp"),
            **spreads
        })
    return pd.DataFrame(rows)

# ---------- DASH APP ----------
app = dash.Dash(__name__)
app.title = "Futures Spread Dashboard"

print(f"[{datetime.now()}] Loading margin/charges cache...")
if not os.path.exists(MARGIN_CSV):
    raise FileNotFoundError(f"{MARGIN_CSV} missing! Run precalc_margin_charges.py first.")
else:
    mtime = datetime.fromtimestamp(os.path.getmtime(MARGIN_CSV))
    age_hrs = (datetime.now() - mtime).total_seconds() / 3600
    if age_hrs > 24:
        print(f"⚠️ {MARGIN_CSV} is {age_hrs:.1f} hrs old. Consider refreshing.")

margin_df = pd.read_csv(MARGIN_CSV)
instrument_data_list = load_instruments()
underlyings = load_underlyings()

subscribe_keys = []
for s in underlyings:
    for f in get_instrument_keys_for_symbol(s, instrument_data_list):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))

#initial_rest_poll(subscribe_keys)
#start_sdk_streamer(subscribe_keys)

# ---------- DASHBOARD LAYOUT ----------
app.layout = html.Div([
    html.H3("Live Futures Spread Dashboard"),
    html.Div(f"Tracking {len(underlyings)} underlyings | {len(subscribe_keys)} contracts"),
    html.Div(id="last_update", style={"margin": "10px 0", "fontWeight": "bold"}),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dash_table.DataTable(
        id="table",
        columns=[
            {"name": "Symbol", "id": "Symbol"},
            {"name": "Lot_Size", "id": "Lot_Size", "type": "numeric"},
            {"name": "Margin", "id": "Margin", "type": "numeric"},
            {"name": "Charges", "id": "Charges", "type": "numeric"},
            {"name": "Interest", "id": "Cost_of_Carry", "type": "numeric"},
            {"name": "Near LTP", "id": "Near_ltp"},
            {"name": "Next LTP", "id": "Next_ltp"},
            {"name": "Far LTP", "id": "Far_ltp"},
            {"name": "Spread_NearBuy_NextSell", "id": "Spread_NearBuy_NextSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NearBuy_NextSell_pct", "type": "numeric"},
            {"name": "Spread_NearSell_NextBuy", "id": "Spread_NearSell_NextBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NearSell_NextBuy_pct", "type": "numeric"},
            {"name": "Spread_NextBuy_FarSell", "id": "Spread_NextBuy_FarSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NextBuy_FarSell_pct", "type": "numeric"},
            {"name": "Spread_NextSell_FarBuy", "id": "Spread_NextSell_FarBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NextSell_FarBuy_pct", "type": "numeric"},
            {"name": "Spread_NearBuy_FarSell", "id": "Spread_NearBuy_FarSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NearBuy_FarSell_pct", "type": "numeric"},
            {"name": "Spread_NearSell_FarBuy", "id": "Spread_NearSell_FarBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NearSell_FarBuy_pct", "type": "numeric"},
        ],
        sort_action="native",
        sort_mode="multi",
        filter_action="native",
        style_table={"overflowX": "auto", "minWidth": "100%", "margin": "10px 0"},
        style_cell={"textAlign": "center", "padding": "4px", "fontFamily": "monospace", "minWidth": "100px"},
        style_header={"backgroundColor": "#111", "color": "white", "fontWeight": "bold", "textAlign": "center"},
        style_filter={"backgroundColor": "#f8f8f8", "padding": "2px", "fontSize": "12px"},
        style_data_conditional=[
            {"if": {"filter_query": f"{{{col}}} > 0", "column_id": col},
             "backgroundColor": "#133d13", "color": "white"}
            for col in [
                "Spread_NearBuy_NextSell", "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell", "Spread_NearSell_NextBuy",
                "Spread_NearSell_FarBuy", "Spread_NextSell_FarBuy"
            ]
        ] + [
            {"if": {"filter_query": f"{{{col}}} < 0", "column_id": col},
             "backgroundColor": "#5a0a0a", "color": "white"}
            for col in [
                "Spread_NearBuy_NextSell", "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell", "Spread_NearSell_NextBuy",
                "Spread_NearSell_FarBuy", "Spread_NextSell_FarBuy"
            ]
        ],
        persistence=True,
        persistence_type="memory",
    )
])



@app.callback(
    [Output("table", "data"), Output("last_update", "children")],
    Input("interval", "n_intervals")
)
def update_table(_):
    print("[CALLBACK] Triggered", flush=True)
    try:
        df = build_df(underlyings, instrument_data_list, margin_df).fillna("")
        print(f"[CALLBACK] Data built with {len(df)} rows", flush=True)
    except Exception as e:
        print(f"[CALLBACK] Error: {e}", flush=True)
        return [], f"Error: {e}"
    now = datetime.now().strftime("%H:%M:%S")
    cached_df.cache_clear()
    return df.to_dict("records"), f"Last updated: {now}"

#server = app.server

#if __name__ == "__main__":
#    app.run(host="0.0.0.0", port=8051, debug=False)

# At the bottom of your file, replace the if __name__ block:

if __name__ == "__main__":
    # Start WebSocket and polling ONLY in the main process
    print(f"[{datetime.now()}] Starting WebSocket and initial poll...")
    initial_rest_poll(subscribe_keys)
    start_sdk_streamer(subscribe_keys)

    app.run(host="0.0.0.0", port=8051, debug=False)

