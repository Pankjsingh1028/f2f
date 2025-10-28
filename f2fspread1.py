import os
import json
import time
import threading
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

import dash
from dash import dcc, html, dash_table, no_update
from dash.dependencies import Input, Output

# Upstox SDK
import upstox_client
from upstox_client.rest import ApiException

# ---------------- CONFIG ----------------
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
REFRESH_INTERVAL = 1000  # 1 second for real-time

# Shared state
market_state = {}
market_state_lock = threading.Lock()

# Volatile trigger for Dash reactivity
last_update_ts = 0.0
last_update_lock = threading.Lock()

# Track updated symbols for optimization (optional)
updated_symbols = set()
updated_symbols_lock = threading.Lock()

# ---------------- HELPERS ----------------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

def extract_symbol_from_key(ik):
    """Extract underlying symbol from instrument_key like NSE_FO|RELIANCE25JUNFUT"""
    return ik.split("|")[1].split("25")[0] if "|" in ik and "25" in ik else None

# ---------------- SDK CALLBACKS ----------------
def on_message(message):
    if message.get("type") != "live_feed":
        return

    feeds = message.get("feeds", {})
    for ik, payload in feeds.items():
        full_feed = payload.get("fullFeed", {})
        market_ff = full_feed.get("marketFF", {})
        depth_list = market_ff.get("marketLevel", {}).get("bidAskQuote", [])

        bid = ask = None
        if depth_list:
            bid = safe_float(depth_list[0].get("bidP"))
            ask = safe_float(depth_list[0].get("askP"))

        with market_state_lock:
            if bid is not None or ask is not None:
                print(f"[WS] {ik} → Bid={bid}, Ask={ask}")
            market_state[ik] = {
                "bidP": bid,
                "askP": ask,
                "ts": datetime.now(timezone.utc).isoformat()
            }

        # Trigger Dash update
        global last_update_ts
        with last_update_lock:
            last_update_ts = time.time()

        # Optional: track symbol for partial update
        sym = extract_symbol_from_key(ik)
        if sym:
            with updated_symbols_lock:
                updated_symbols.add(sym)

def on_open():
    print(f"[{datetime.now()}] SDK WS Connected & Subscribed")

def on_error(error):
    print(f"[{datetime.now()}] SDK Error: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] SDK Reconnecting...")

# ---------------- SDK STREAMER ----------------
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
            time.sleep(1)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

# ---------------- DATA FUNCTIONS ----------------
def get_state(ik, tsym):
    with market_state_lock:
        s = market_state.get(ik, {})
        return {"symbol": tsym, "bidP": s.get("bidP"), "askP": s.get("askP")}

def compute_spreads(near, nxt, far):
    return {
        "Spread_NearBuy_NextSell": diff(nxt.get("bidP"), near.get("askP")),
        "Spread_NearBuy_FarSell": diff(far.get("bidP"), near.get("askP")),
        "Spread_NextBuy_FarSell": diff(far.get("bidP"), nxt.get("askP")),
        "Spread_NextBuy_NearSell": diff(near.get("bidP"), nxt.get("askP")),
        "Spread_FarBuy_NearSell": diff(near.get("bidP"), far.get("askP")),
        "Spread_FarBuy_NextSell": diff(nxt.get("bidP"), far.get("askP")),
    }

def load_instruments():
    with open(INSTRUMENTS_JSON, "r") as f:
        return json.load(f)

def load_underlyings():
    df = pd.read_csv(STOCKS_CSV)
    return df["underlying_symbol"].dropna().unique().tolist()

def get_instrument_keys_for_symbol(symbol, instrument_data):
    futures = []
    for instrument in instrument_data:
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

def build_df(underlyings, instrument_data):
    rows = []
    for sym in underlyings:
        futs = get_instrument_keys_for_symbol(sym, instrument_data)
        near, nxt, far = (futs + [None]*3)[:3]
        near_s = get_state(near["instrument_key"], near["trading_symbol"]) if near else {}
        nxt_s = get_state(nxt["instrument_key"], nxt["trading_symbol"]) if nxt else {}
        far_s = get_state(far["instrument_key"], far["trading_symbol"]) if far else {}
        spreads = compute_spreads(near_s, nxt_s, far_s)
        rows.append({
            "Symbol": sym,
            "Near": near_s.get("symbol"), "Near_bid": near_s.get("bidP"), "Near_ask": near_s.get("askP"),
            "Next": nxt_s.get("symbol"), "Next_bid": nxt_s.get("bidP"), "Next_ask": nxt_s.get("askP"),
            "Far": far_s.get("symbol"), "Far_bid": far_s.get("bidP"), "Far_ask": far_s.get("askP"),
            **spreads,
        })
    return pd.DataFrame(rows)

# ---------------- DASH APP ----------------
app = dash.Dash(__name__)
app.title = "Futures Spread Dashboard"

# Load static data
instrument_data = load_instruments()
underlyings = load_underlyings()

# Subscribe to all futures
subscribe_keys = []
for s in underlyings:
    for f in get_instrument_keys_for_symbol(s, instrument_data):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))
print(f"Subscribing to {len(subscribe_keys)} instruments via SDK")
start_sdk_streamer(subscribe_keys)

# Layout
app.layout = html.Div([
    html.H3("Live Futures Spread Dashboard", style={"textAlign": "center"}),
    html.Div(f"Tracking {len(underlyings)} underlyings | {len(subscribe_keys)} contracts",
             style={"textAlign": "center", "margin": "10px"}),
    html.Div(id="last_update", style={"textAlign": "center", "margin": "10px", "fontWeight": "bold"}),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dash_table.DataTable(
        id="table",
        columns=[
            {"name": i, "id": i, "type": "numeric" if "Spread" in i or "_bid" in i or "_ask" in i else "text"}
            for i in [
                "Symbol", "Near", "Near_bid", "Near_ask",
                "Next", "Next_bid", "Next_ask",
                "Far", "Far_bid", "Far_ask",
                "Spread_NearBuy_NextSell", "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell", "Spread_NextBuy_NearSell",
                "Spread_FarBuy_NearSell", "Spread_FarBuy_NextSell"
            ]
        ],
        sort_action="native",
        sort_mode="multi",
        filter_action="native",
        filter_query="",
        style_table={"overflowX": "auto", "minWidth": "100%", "margin": "10px 0"},
        style_cell={
            "textAlign": "center",
            "padding": "4px",
            "fontFamily": "monospace",
            "minWidth": "100px",
        },
        style_header={
            "backgroundColor": "#111",
            "color": "white",
            "fontWeight": "bold",
        },
        style_filter={
            "backgroundColor": "#f8f8f8",
            "padding": "2px",
            "fontSize": "12px",
        },
        style_data_conditional=[
            {
                "if": {"filter_query": f"{{{col}}} > 0", "column_id": col},
                "backgroundColor": "#133d13",
                "color": "white"
            } for col in [
                "Spread_NearBuy_NextSell", "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell", "Spread_NextBuy_NearSell",
                "Spread_FarBuy_NearSell", "Spread_FarBuy_NextSell"
            ]
        ] + [
            {
                "if": {"filter_query": f"{{{col}}} < 0", "column_id": col},
                "backgroundColor": "#5a0a0a",
                "color": "white"
            } for col in [
                "Spread_NearBuy_NextSell", "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell", "Spread_NextBuy_NearSell",
                "Spread_FarBuy_NearSell", "Spread_FarBuy_NextSell"
            ]
        ],
        persistence=True,
        persistence_type="memory",
    )
])

# ---------------- CALLBACK ----------------
@app.callback(
    [Output("table", "data"), Output("last_update", "children")],
    Input("interval", "n_intervals")
)
def update_table(_):
    global last_update_ts

    # Capture latest trigger
    with last_update_lock:
        trigger_ts = last_update_ts

    # Optional: skip if no updates
    with updated_symbols_lock:
        if not updated_symbols and trigger_ts == 0:
            return no_update, no_update

    df = build_df(underlyings, instrument_data)
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[DASH] Updated @ {now} | Rows: {len(df)} | Trigger: {trigger_ts:.2f}")

    return df.to_dict("records"), f"Last updated: {now}"

# ---------------- RUN ----------------
if __name__ == "__main__":
    # For local testing only
    app.run(host="0.0.0.0", port=8051, debug=False)
