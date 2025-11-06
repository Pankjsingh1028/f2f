import os
import json
import time
import threading
import tempfile
import pandas as pd
import requests
from datetime import datetime
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
MARKET_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"

# Where WS process writes shared state snapshot
STATE_PATH = os.environ.get("STATE_PATH", "/var/run/f2f/market_state.json")

market_state = {}
market_state_lock = threading.Lock()

# ---------- HELPERS ----------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

def snapshot_state_to_file():
    """Atomically persist market_state for Dash workers to read."""
    try:
        with market_state_lock:
            snap = json.dumps(market_state, ensure_ascii=False)

        d = os.path.dirname(STATE_PATH)
        os.makedirs(d, exist_ok=True)

        fd, tmp = tempfile.mkstemp(prefix=".state.", dir=d)
        with os.fdopen(fd, "w") as f:
            f.write(snap)

        os.replace(tmp, STATE_PATH)
    except Exception as e:
        print(f"[state] snapshot error: {e}")

def load_state_from_file():
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                return json.load(f)
    except Exception as e:
        print(f"[state] load error: {e}")

    with market_state_lock:
        return dict(market_state)

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
                "trading_symbol": instrument.get("trading_symbol"),
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
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                with market_state_lock:
                    for ik, quote in data.items():
                        depth = quote.get("depth", {})
                        buy_depth = depth.get("buy", [])
                        sell_depth = depth.get("sell", [])
                        bid = safe_float(buy_depth[0].get("price")) if buy_depth else None
                        ask = safe_float(sell_depth[0].get("price")) if sell_depth else None
                        ltp = safe_float(quote.get("last_price"))
                        market_state[ik] = {"bidP": bid, "askP": ask, "ltp": ltp}
            else:
                print("[REST] HTTP", resp.status_code)
        except Exception as e:
            print("[REST] Error:", e)

        time.sleep(0.1)

    print(f"[{datetime.now()}] Initial poll complete.")

# ---------- WEBSOCKET ----------
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

def on_open():
    print(f"[{datetime.now()}] WebSocket connected")

def on_error(e):
    print("[WS] Error:", e)

def on_reconnecting():
    print("[WS] Reconnecting...")

def start_sdk_streamer(subscribe_keys):
    def _run():
        cfg = upstox_client.Configuration()
        cfg.access_token = ACCESS_TOKEN
        api = upstox_client.ApiClient(cfg)

        streamer = upstox_client.MarketDataStreamerV3(api, subscribe_keys, "full")
        streamer.on("message", on_message)
        streamer.on("open", on_open)
        streamer.on("error", on_error)
        streamer.on("reconnecting", on_reconnecting)
        streamer.auto_reconnect(True, 5, 5)
        streamer.connect()

        while True:
            time.sleep(1)

    threading.Thread(target=_run, daemon=True).start()

def bootstrap_market_data():
    initial_rest_poll(subscribe_keys)
    start_sdk_streamer(subscribe_keys)

# ---------- TABLE ----------
def get_state(ik):
    state = load_state_from_file()
    return state.get(ik, {"ltp": None, "bidP": None, "askP": None})

def compute_spreads(near, nxt, far):
    near_ltp = safe_float(near.get("ltp")) or 1

    def pct(a):
        if a is None:
            return None
        return round((a / near_ltp) * 100, 2)

    abs_spreads = {
        "Spread_NearBuy_NextSell": diff(nxt.get("bidP"), near.get("askP")),
        "Spread_NearSell_NextBuy": diff(near.get("bidP"), nxt.get("askP")),
        "Spread_NextBuy_FarSell": diff(far.get("bidP"), nxt.get("askP")),
        "Spread_NextSell_FarBuy": diff(nxt.get("bidP"), far.get("askP")),
        "Spread_NearBuy_FarSell": diff(far.get("bidP"), near.get("askP")),
        "Spread_NearSell_FarBuy": diff(near.get("bidP"), far.get("askP")),
    }

    pct_spreads = {k + "_pct": pct(v) for k, v in abs_spreads.items()}
    return {**abs_spreads, **pct_spreads}

def build_df(underlyings, instruments, margin_df):
    rows = []
    for sym in underlyings:
        futs = get_instrument_keys_for_symbol(sym, instruments)
        if len(futs) < 3:
            futs += [None] * (3 - len(futs))

        near, nxt, far = futs
        near_s = get_state(near["instrument_key"]) if near else {}
        nxt_s = get_state(nxt["instrument_key"]) if nxt else {}
        far_s = get_state(far["instrument_key"]) if far else {}

        spreads = compute_spreads(near_s, nxt_s, far_s)

        m = margin_df.loc[margin_df["Symbol"] == sym]
        if not m.empty:
            margin = float(m["Margin"].iloc[0])
            charges = float(m["Charges"].iloc[0])
            carry = float(m["Cost_of_Carry"].iloc[0])
            lot_size = int(m["Lot_Size"].iloc[0])
        else:
            margin = charges = carry = lot_size = None

        rows.append({
            "Symbol": sym,
            "Lot_Size": lot_size,
            "Margin": margin,
            "Charges": charges,
            "Cost_of_Carry": carry,
            "Near_ltp": near_s.get("ltp"),
            "Next_ltp": nxt_s.get("ltp"),
            "Far_ltp": far_s.get("ltp"),
            **spreads,
        })

    return pd.DataFrame(rows)

# ---------- DASH APP ----------
print(f"[{datetime.now()}] Loading static files...")
margin_df = pd.read_csv(MARGIN_CSV)
instrument_data_list = load_instruments()
underlyings = load_underlyings()

subscribe_keys = []
for s in underlyings:
    for f in get_instrument_keys_for_symbol(s, instrument_data_list):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))

app = dash.Dash(__name__)
app.title = "Futures Spread Dashboard"

from functools import lru_cache

@lru_cache(maxsize=1)
def cached_df():
    return build_df(underlyings, instrument_data_list, margin_df)

app.layout = html.Div([
    html.H3("Live Futures Spread Dashboard"),
    html.Div(f"Tracking {len(underlyings)} underlyings | {len(subscribe_keys)} contracts"),
    html.Div(id="last_update", style={"fontWeight": "bold", "margin": "10px 0"}),
    dcc.Interval(id="interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dash_table.DataTable(
        id="table",
        data=[],
        columns=[{"name": c, "id": c} for c in [
            "Symbol", "Lot_Size", "Margin", "Charges", "Cost_of_Carry",
            "Near_ltp", "Next_ltp", "Far_ltp",
            "Spread_NearBuy_NextSell", "Spread_NearBuy_NextSell_pct",
            "Spread_NearSell_NextBuy", "Spread_NearSell_NextBuy_pct",
            "Spread_NextBuy_FarSell", "Spread_NextBuy_FarSell_pct",
            "Spread_NextSell_FarBuy", "Spread_NextSell_FarBuy_pct",
            "Spread_NearBuy_FarSell", "Spread_NearBuy_FarSell_pct",
            "Spread_NearSell_FarBuy", "Spread_NearSell_FarBuy_pct",
        ]],
        style_table={"overflowX": "auto"},
    )
])

@app.callback(
    [Output("table", "data"), Output("last_update", "children")],
    Input("interval", "n_intervals"),
)
def update_table(_):
    df = cached_df()
    cached_df.cache_clear()
    now = datetime.now().strftime("%H:%M:%S")
    return df.to_dict("records"), f"Last update: {now}"

server = app.server

# ---------- DEV MODE ----------
if __name__ == "__main__":
    bootstrap_market_data()

    # Periodically save snapshot → Dash reads it
    def _snapper():
        while True:
            snapshot_state_to_file()
            time.sleep(0.5)

    threading.Thread(target=_snapper, daemon=True).start()

    app.run(host="0.0.0.0", port=8051, debug=False)

