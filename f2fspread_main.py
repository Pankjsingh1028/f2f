# f2fspread_main.py
import os
import json
import time
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output

import upstox_client

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

# Shared state + eventlet-friendly lock
market_state = {}
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
        print(f"[{datetime.now()}] ERROR: {INSTRUMENTS_JSON} not found.")
        return []

def load_underlyings():
    try:
        df = pd.read_csv(STOCKS_CSV)
        return df["underlying_symbol"].dropna().unique().tolist()
    except FileNotFoundError:
        print(f"[{datetime.now()}] ERROR: {STOCKS_CSV} not found.")
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
                "instrument_key": str(instrument.get("instrument_key")),  # str() for consistency
                "expiry": instrument.get("expiry", 0),
                "trading_symbol": instrument.get("trading_symbol")
            })
    futures.sort(key=lambda x: x["expiry"])
    return futures[:3]

# ---------- REST POLLING ----------
def initial_rest_poll(subscribe_keys):
    print(f"[{datetime.now()}] [POLL] Starting initial REST poll for {len(subscribe_keys)} keys...")
    batch_size = 490
    for i in range(0, len(subscribe_keys), batch_size):
        keys_batch = subscribe_keys[i:i + batch_size]
        url = f"{MARKET_QUOTE_URL}?instrument_key={','.join(keys_batch)}"
        headers = {"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                with market_state_lock:
                    for ik_long, quote in data.items():
                        ik = str(quote.get("instrument_token") or ik_long)  # str()
                        depth = quote.get("depth", {})
                        buy_depth = depth.get("buy", [])
                        sell_depth = depth.get("sell", [])
                        bid = safe_float(buy_depth[0].get("price")) if buy_depth else None
                        ask = safe_float(sell_depth[0].get("price")) if sell_depth else None
                        ltp = safe_float(quote.get("last_price"))
                        market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}
                print(f"  [POLL] Batch {i//batch_size + 1} OK ({len(keys_batch)})")
            else:
                print(f"  [POLL] HTTP {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            print(f"  [POLL] Error: {e}")
        time.sleep(0.1)
    print(f"[{datetime.now()}] [POLL] Initial poll complete. {len(market_state)} instruments cached.")

# ---------- WEBSOCKET ----------
def on_message(message):
    if message.get("type") != "live_feed":
        return
    feeds = message.get("feeds", {})
    if not feeds:
        return
    print(f"[{datetime.now()}] [WS] Received {len(feeds)} updates")
    with market_state_lock:
        for ik_raw, payload in feeds.items():
            ik = str(ik_raw)  # Critical: str()
            ff = payload.get("fullFeed", {}).get("marketFF", {})
            ltpc = ff.get("ltpc", {})
            ltp = safe_float(ltpc.get("ltp"))
            depth_list = ff.get("marketLevel", {}).get("bidAskQuote", [])
            bid = safe_float(depth_list[0].get("bidP")) if depth_list else None
            ask = safe_float(depth_list[0].get("askP")) if depth_list else None
            market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}

def on_open():
    print(f"[{datetime.now()}] [WS] Connected")

def on_error(error):
    print(f"[{datetime.now()}] [WS] ERROR: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] [WS] Reconnecting...")

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
        print(f"[{datetime.now()}] [WS] Streamer started")
        while True:
            eventlet.sleep(1)
    import threading
    t = threading.Thread(target=_run, daemon=True)
    t.start()

# ---------- CORE LOGIC ----------
def compute_spreads(near, nxt, far):
    near_ltp = safe_float(near.get("ltp")) or 1
    def pct(d):
        return round((d / near_ltp) * 100, 2) if d is not None else None

    spreads = {
        "Spread_NearBuy_NextSell": diff(nxt.get("bidP"), near.get("askP")),
        "Spread_NearSell_NextBuy": diff(near.get("bidP"), nxt.get("askP")),
        "Spread_NextBuy_FarSell": diff(far.get("bidP"), nxt.get("askP")),
        "Spread_NextSell_FarBuy": diff(nxt.get("bidP"), far.get("askP")),
        "Spread_NearBuy_FarSell": diff(far.get("bidP"), near.get("askP")),
        "Spread_NearSell_FarBuy": diff(near.get("bidP"), far.get("askP")),
    }
    pct_spreads = {k + "_pct": pct(v) for k, v in spreads.items()}
    return {**spreads, **pct_spreads}

def get_state(ik):
    ik = str(ik)  # Critical: str()
    with market_state_lock:
        return market_state.get(ik, {"ltp": None, "bidP": None, "askP": None})

def build_df(underlyings, instruments, margin_df):
    print(f"[{datetime.now()}] [BUILD] underlyings={len(underlyings)}, state={len(market_state)}")
    rows = []
    for sym in underlyings:
        futs = get_instrument_keys_for_symbol(sym, instruments)[:3]
        futs += [None] * (3 - len(futs))
        near, nxt, far = futs

        near_s = get_state(near["instrument_key"]) if near else {}
        nxt_s = get_state(nxt["instrument_key"]) if nxt else {}
        far_s = get_state(far["instrument_key"]) if far else {}

        spreads = compute_spreads(near_s, nxt_s, far_s)

        mrow = margin_df.loc[margin_df["Symbol"] == sym]
        margin = charges = carry = lot_size = None
        if not mrow.empty:
            margin = float(mrow["Margin"].iloc[0])
            charges = float(mrow["Charges"].iloc[0])
            carry = float(mrow["Cost_of_Carry"].iloc[0])
            lot_size = int(mrow["Lot_Size"].iloc[0])

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

# Load static data at import time
print(f"[{datetime.now()}] [INIT] Loading static files...")
try:
    margin_df = pd.read_csv(MARGIN_CSV)
    instrument_data_list = load_instruments()
    underlyings = load_underlyings()
    print(f"[{datetime.now()}] [INIT] Loaded: {len(underlyings)} symbols, {len(instrument_data_list)} instruments")
except Exception as e:
    print(f"[{datetime.now()}] [INIT] FAILED TO LOAD DATA: {e}")
    margin_df = pd.DataFrame()
    instrument_data_list = []
    underlyings = []

# Layout
app.layout = html.Div([
    html.H3("Live Futures Spread Dashboard"),
    html.Div(id="status", style={"margin": "10px 0", "fontWeight": "bold"}),
    html.Div(id="last_update", style={"margin": "10px 0", "fontWeight": "bold"}),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dash_table.DataTable(
        id="table",
        columns=[
            {"name": "Symbol", "id": "Symbol"},
            {"name": "Lot", "id": "Lot_Size", "type": "numeric"},
            {"name": "Margin", "id": "Margin", "type": "numeric"},
            {"name": "Charges", "id": "Charges", "type": "numeric"},
            {"name": "Carry", "id": "Cost_of_Carry", "type": "numeric"},
            {"name": "Near", "id": "Near_ltp"},
            {"name": "Next", "id": "Next_ltp"},
            {"name": "Far", "id": "Far_ltp"},
            {"name": "N→X", "id": "Spread_NearBuy_NextSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NearBuy_NextSell_pct", "type": "numeric"},
            {"name": "X→N", "id": "Spread_NearSell_NextBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NearSell_NextBuy_pct", "type": "numeric"},
            {"name": "X→F", "id": "Spread_NextBuy_FarSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NextBuy_FarSell_pct", "type": "numeric"},
            {"name": "F→X", "id": "Spread_NextSell_FarBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NextSell_FarBuy_pct", "type": "numeric"},
            {"name": "N→F", "id": "Spread_NearBuy_FarSell", "type": "numeric"},
            {"name": "%", "id": "Spread_NearBuy_FarSell_pct", "type": "numeric"},
            {"name": "F→N", "id": "Spread_NearSell_FarBuy", "type": "numeric"},
            {"name": "%", "id": "Spread_NearSell_FarBuy_pct", "type": "numeric"},
        ],
        sort_action="native",
        filter_action="native",
        page_size=50,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "padding": "4px", "fontFamily": "monospace"},
        style_header={"backgroundColor": "#111", "color": "white", "fontWeight": "bold"},
        style_data_conditional=[
            {"if": {"filter_query": f"{{{col}}} > 0", "column_id": col},
             "backgroundColor": "#133d13", "color": "white"}
            for col in ["Spread_NearBuy_NextSell", "Spread_NextBuy_FarSell", "Spread_NearBuy_FarSell",
                        "Spread_NearSell_NextBuy", "Spread_NearSell_FarBuy", "Spread_NextSell_FarBuy"]
        ] + [
            {"if": {"filter_query": f"{{{col}}} < 0", "column_id": col},
             "backgroundColor": "#5a0a0a", "color": "white"}
            for col in ["Spread_NearBuy_NextSell", "Spread_NextBuy_FarSell", "Spread_NearBuy_FarSell",
                        "Spread_NearSell_NextBuy", "Spread_NearSell_FarBuy", "Spread_NextSell_FarBuy"]
        ],
    )
])

# ---------- CALLBACK ----------
@app.callback(
    [Output("table", "data"), Output("last_update", "children"), Output("status", "children")],
    Input("interval", "n_intervals")
)
def update_table(_):
    print(f"[{datetime.now()}] [DASH] Callback triggered")
    try:
        df = build_df(underlyings, instrument_data_list, margin_df).fillna("")
        print(f"[{datetime.now()}] [DASH] Table built: {len(df)} rows")
    except Exception as e:
        print(f"[{datetime.now()}] [DASH] ERROR: {e}")
        return [], f"Error: {e}", f"ERROR: {e}"
    
    now = datetime.now().strftime("%H:%M:%S")
    status = f"Tracking {len(underlyings)} symbols | {len(market_state)} live"
    return df.to_dict("records"), f"Updated: {now}", status
