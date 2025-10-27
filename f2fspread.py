import os
import json
import time
import threading
import queue  # Not used now, but kept for compatibility
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output

# Upstox SDK imports
import upstox_client
from upstox_client.rest import ApiException

# ---------------- CONFIG ----------------
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")
INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
REFRESH_INTERVAL = 5000  # ms

market_state = {}
market_state_lock = threading.Lock()

# ---------------- HELPERS ----------------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

# ---------------- SDK CALLBACK ----------------
def on_message(message):
    """SDK callback: Parse live_feed and update market_state."""
    if message.get("type") != "live_feed":
        return  # Skip market_info, etc.

    feeds = message.get("feeds", {})
    for ik, payload in feeds.items():
        # Extract bid/ask from fullFeed (matches your proto path)
        full_feed = payload.get("fullFeed", {})
        market_ff = full_feed.get("marketFF", {})
        depth_list = market_ff.get("marketLevel", {}).get("bidAskQuote", [])
        
        bid = None
        ask = None
        if depth_list:
            bid = safe_float(depth_list[0].get("bidP"))
            ask = safe_float(depth_list[0].get("askP"))
        
        with market_state_lock:
            if bid is not None or ask is not None:
                print(f"[{datetime.now()}] STATE UPDATE {ik}: Bid={bid}, Ask={ask}")
            market_state[ik] = {"bidP": bid, "askP": ask, "ts": datetime.now(timezone.utc).isoformat()}

def on_open():
    print(f"[{datetime.now()}] SDK WS Connected & Subscribed")

def on_error(error):
    print(f"[{datetime.now()}] SDK Error: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] SDK Reconnecting...")

# ---------------- SDK SETUP ----------------
def start_sdk_streamer(subscribe_keys):
    """Start MarketDataStreamerV3 in a thread."""
    def _run():
        configuration = upstox_client.Configuration()
        configuration.access_token = ACCESS_TOKEN

        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client, subscribe_keys, "full")

        streamer.on("message", on_message)
        streamer.on("open", on_open)
        streamer.on("error", on_error)
        streamer.on("reconnecting", on_reconnecting)

        # Enable auto-reconnect: True, 5s interval, max 5 retries
        streamer.auto_reconnect(True, 5, 5)

        streamer.connect()

        # Keep thread alive
        while True:
            time.sleep(1)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

# ---------------- EXISTING FUNCTIONS (UNCHANGED) ----------------
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
    """Return the 3 nearest FUT contracts for the symbol, using correct Upstox keys."""
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

# ---------------- DASH APP (UNCHANGED) ----------------
app = dash.Dash(__name__)
app.title = "Futures/Options Spread Dashboard"

instrument_data = load_instruments()
underlyings = load_underlyings()

subscribe_keys = []
for s in underlyings:
    for f in get_instrument_keys_for_symbol(s, instrument_data):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))
print(f"Subscribing to {len(subscribe_keys)} instruments via SDK")
start_sdk_streamer(subscribe_keys)

app.layout = html.Div([
    html.H3("Live Futures Spread Dashboard"),
    html.Div(f"Tracking {len(underlyings)} underlyings | {len(subscribe_keys)} contracts"),
    html.Div(id="last_update", style={"margin": "10px 0", "fontWeight": "bold"}),
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
        # Enable sorting
        sort_action="native",  # Allows clicking headers to sort
        sort_mode="multi",     # Supports multi-column sorting (e.g., Symbol then Near_bid)
        
        # Enable filtering
        filter_action="native",  # Adds filter inputs below headers
        filter_query="",         # Start with no filters applied
        
        # Styling for table and filters
        style_table={
            "overflowX": "auto",
            "minWidth": "100%",
            "margin": "10px 0",
        },
        style_cell={
            "textAlign": "center",
            "padding": "4px",
            "fontFamily": "monospace",
            "minWidth": "100px",  # Ensure columns are wide enough
        },
        style_header={
            "backgroundColor": "#111",
            "color": "white",
            "fontWeight": "bold",
            "textAlign": "center",
        },
        style_filter={
            "backgroundColor": "#f8f8f8",  # Light background for filter inputs
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
        #page_size=30,
        
        # Persist user sorting/filtering across updates
        persistence=True,
        persistence_type="memory",  # Options: "memory", "session", "local"
    )
])

@app.callback(
    [Output("table", "data"), Output("last_update", "children")],
    Input("interval", "n_intervals")
)
def update_table(_):
    df = build_df(underlyings, instrument_data)
    now = datetime.now().strftime("%H:%M:%S")
    return df.to_dict("records"), f"Last updated: {now}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)