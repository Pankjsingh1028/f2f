# f2fspread_main.py
import os
import json
import time
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict

import dash
from dash import dcc, html, dash_table, ctx, callback, Output, Input, State, ALL
from dash.exceptions import PreventUpdate
import dash

import upstox_client
import eventlet

# ---------- CONFIG ----------
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
MARGIN_CSV = "margin_charges_cache.csv"
REFRESH_INTERVAL = 300  # 300ms
MARKET_QUOTE_URL = 'https://api.upstox.com/v2/market-quote/quotes'

# Global state
market_state = {}
market_state_lock = eventlet.semaphore.Semaphore()
last_update = {}
symbol_to_keys = {}

# ---------- HELPERS ----------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

# ---------- LOAD STATIC ----------
def load_static():
    global symbol_to_keys
    margin_df = pd.read_csv(MARGIN_CSV)
    instruments = json.load(open(INSTRUMENTS_JSON))
    underlyings = pd.read_csv(STOCKS_CSV)["underlying_symbol"].dropna().unique()

    symbol_to_keys = {}
    for sym in underlyings:
        futs = [
            str(inst["instrument_key"])
            for inst in instruments
            if inst.get("segment") == "NSE_FO"
            and inst.get("instrument_type") == "FUT"
            and inst.get("underlying_symbol") == sym
        ][:3]
        futs += [None] * (3 - len(futs))
        symbol_to_keys[sym] = futs

    return margin_df, list(underlyings)

margin_df, underlyings = load_static()

# ---------- REST POLLING ----------
def initial_rest_poll(subscribe_keys):
    print(f"[{datetime.now()}] [POLL] Initial poll: {len(subscribe_keys)} keys")
    batch_size = 490
    for i in range(0, len(subscribe_keys), batch_size):
        batch = subscribe_keys[i:i + batch_size]
        url = f"{MARKET_QUOTE_URL}?instrument_key={','.join(batch)}"
        headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                with market_state_lock:
                    for ik_long, quote in data.items():
                        ik = str(quote.get("instrument_token") or ik_long)
                        depth = quote.get("depth", {})
                        buy = depth.get("buy", [])
                        sell = depth.get("sell", [])
                        bid = safe_float(buy[0].get("price")) if buy else None
                        ask = safe_float(sell[0].get("price")) if sell else None
                        ltp = safe_float(quote.get("last_price"))
                        market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}
                print(f"  [POLL] Batch {i//batch_size + 1} OK")
            else:
                print(f"  [POLL] HTTP {resp.status_code}")
        except Exception as e:
            print(f"  [POLL] Error: {e}")
        time.sleep(0.1)
    print(f"[{datetime.now()}] [POLL] Complete. {len(market_state)} cached.")

# ---------- WEBSOCKET ----------
def on_message(message):
    if message.get("type") != "live_feed":
        return
    feeds = message.get("feeds", {})
    if not feeds:
        return
    print(f"[{datetime.now()}] [WS] {len(feeds)} updates")
    with market_state_lock:
        for ik_raw, payload in feeds.items():
            ik = str(ik_raw)
            ff = payload.get("fullFeed", {}).get("marketFF", {})
            ltpc = ff.get("ltpc", {})
            ltp = safe_float(ltpc.get("ltp"))
            depth = ff.get("marketLevel", {}).get("bidAskQuote", [])
            bid = safe_float(depth[0].get("bidP")) if depth else None
            ask = safe_float(depth[0].get("askP")) if depth else None
            market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}
            last_update[ik] = time.time()

def on_open():
    print(f"[{datetime.now()}] [WS] Connected")

def on_error(error):
    print(f"[{datetime.now()}] [WS] ERROR: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] [WS] Reconnecting...")

def start_streamer():
    subscribe_keys = [k for sym in underlyings for k in symbol_to_keys[sym] if k]
    print(f"[{datetime.now()}] [WS] Subscribing to {len(subscribe_keys)} contracts")

    # Initial poll
    initial_rest_poll(subscribe_keys)

    # Start WebSocket
    def _run():
        config = upstox_client.Configuration()
        config.access_token = ACCESS_TOKEN
        api_client = upstox_client.ApiClient(config)
        streamer = upstox_client.MarketDataStreamerV3(api_client, subscribe_keys, "full")
        streamer.on("message", on_message)
        streamer.on("open", on_open)
        streamer.on("error", on_error)
        streamer.on("reconnecting", on_reconnecting)
        streamer.auto_reconnect(True, 5, 5)
        streamer.connect()
        print(f"[{datetime.now()}] [WS] Streamer running")
        while True:
            eventlet.sleep(1)

    import threading
    t = threading.Thread(target=_run, daemon=True)
    t.start()

# ---------- FAST BUILD ----------
def get_row(sym):
    near_k, nxt_k, far_k = symbol_to_keys[sym]
    near = market_state.get(near_k, {}) if near_k else {}
    nxt = market_state.get(nxt_k, {}) if nxt_k else {}
    far = market_state.get(far_k, {}) if Far_k else {}

    near_ltp = safe_float(near.get("ltp")) or 1
    def pct(d): return round(d / near_ltp * 100, 2) if d is not None else None

    spreads = {
        "N→X": diff(nxt.get("bidP"), near.get("askP")),
        "X→N": diff(near.get("bidP"), nxt.get("askP")),
        "X→F": diff(far.get("bidP"), nxt.get("askP")),
        "F→X": diff(nxt.get("bidP"), far.get("askP")),
        "N→F": diff(far.get("bidP"), near.get("askP")),
        "F→N": diff(near.get("bidP"), far.get("askP")),
    }
    spreads = {k: v for k, v in spreads.items() if v is not None}
    pct_spreads = {f"{k}%": pct(v) for k, v in spreads.items()}

    m = margin_df[margin_df["Symbol"] == sym].iloc[0]
    return {
        "Symbol": sym,
        "Lot": int(m["Lot_Size"]),
        "Margin": round(m["Margin"], 2),
        "Charges": round(m["Charges"] / m["Lot_Size"], 2),
        "Carry": round(m["Cost_of_Carry"] / m["Lot_Size"], 2),
        "Near": near.get("ltp"),
        "Next": nxt.get("ltp"),
        "Far": far.get("ltp"),
        **spreads,
        **pct_spreads,
        "_ts": time.time()
    }

# ---------- DASH APP ----------
app = dash.Dash(__name__)
app.title = "F2F Live"

app.layout = html.Div([
    html.H3("Futures Spread — LIVE"),
    html.Div(id="status", style={"margin": "10px 0", "fontWeight": "bold", "color": "#0f0"}),
    dcc.Interval(id="fast-interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dash_table.DataTable(
        id="table",
        columns=[
            {"name": "Sym", "id": "Symbol"},
            {"name": "Lot", "id": "Lot", "type": "numeric"},
            {"name": "Mrg", "id": "Margin", "type": "numeric"},
            {"name": "Chg", "id": "Charges", "type": "numeric"},
            {"name": "Cry", "id": "Carry", "type": "numeric"},
            {"name": "N", "id": "Near"},
            {"name": "X", "id": "Next"},
            {"name": "F", "id": "Far"},
            {"name": "N→X", "id": "N→X", "type": "numeric"},
            {"name": "%", "id": "N→X%", "type": "numeric"},
            {"name": "X→N", "id": "X→N", "type": "numeric"},
            {"name": "%", "id": "X→N%", "type": "numeric"},
            {"name": "X→F", "id": "X→F", "type": "numeric"},
            {"name": "%", "id": "X→F%", "type": "numeric"},
            {"name": "F→X", "id": "F→X", "type": "numeric"},
            {"name": "%", "id": "F→X%", "type": "numeric"},
            {"name": "N→F", "id": "N→F", "type": "numeric"},
            {"name": "%", "id": "N→F%", "type": "numeric"},
            {"name": "F→N", "id": "F→N", "type": "numeric"},
            {"name": "%", "id": "F→N%", "type": "numeric"},
        ],
        data=[],
        page_size=100,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "fontFamily": "monospace"},
        style_header={"backgroundColor": "#111", "color": "white"},
        style_data_conditional=[
            {"if": {"filter_query": "{N→X} > 0"}, "backgroundColor": "#133d13", "color": "white"},
            {"if": {"filter_query": "{N→X} < 0"}, "backgroundColor": "#5a0a0a", "color": "white"},
        ] + [
            {"if": {"filter_query": f"{{{k}}} > 0"}, "backgroundColor": "#133d13", "color": "white"}
            for k in ["X→F", "N→F"]
        ] + [
            {"if": {"filter_query": f"{{{k}}} < 0"}, "backgroundColor": "#5a0a0a", "color": "white"}
            for k in ["X→N", "F→X", "F→N"]
        ]
    )
])

# ---------- FAST CALLBACK ----------
@app.callback(
    Output("table", "data"),
    Output("status", "children"),
    Input("fast-interval", "n_intervals"),
    State("table", "data")
)
def update_fast(n_intervals, current_data):
    if current_data is None:
        current_data = []

    now = datetime.now().strftime("%H:%M:%S")
    live_count = len(market_state)
    status = f"Live: {live_count} contracts | Updated: {now}"

    # First load
    if not current_data:
        print(f"[{datetime.now()}] [DASH] Initial load")
        full_data = [get_row(sym) for sym in underlyings]
        return full_data, status

    # Patch
    patch = {}
    for i, sym in enumerate(underlyings):
        new_row = get_row(sym)
        if i >= len(current_data):
            patch[i] = new_row
            continue
        old_row = current_data[i]
        if (new_row["Near"] != old_row.get("Near") or
            new_row["Next"] != old_row.get("Next") or
            new_row["Far"] != old_row.get("Far") or
            new_row.get("N→X") != old_row.get("N→X")):
            patch[i] = new_row

    if patch:
        print(f"[{datetime.now()}] [DASH] Patching {len(patch)} rows")
        return patch, status
    return dash.no_update, status
