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
REFRESH_INTERVAL = 300  # 300ms = 3.3 updates/sec
MARKET_QUOTE_URL = 'https://api.upstox.com/v2/market-quote/quotes'

# Global state
market_state = {}
market_state_lock = eventlet.semaphore.Semaphore()
last_update = {}
symbol_to_keys = {}

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

# ---------- WEBSOCKET ----------
def on_message(message):
    if message.get("type") != "live_feed":
        return
    feeds = message.get("feeds", {})
    if not feeds:
        return

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

def start_streamer():
    subscribe_keys = [k for sym in underlyings for k in symbol_to_keys[sym] if k]
    # ... same as before ...
    # initial_rest_poll(subscribe_keys)
    # start_sdk_streamer(subscribe_keys)

# ---------- FAST BUILD ----------
def get_row(sym):
    near_k, nxt_k, far_k = symbol_to_keys[sym]
    near = market_state.get(near_k, {}) if near_k else {}
    nxt = market_state.get(nxt_k, {}) if nxt_k else {}
    far = market_state.get(far_k, {}) if far_k else {}

    near_ltp = safe_float(near.get("ltp")) or 1
    def pct(d): return round(d / near_ltp * 100, 2) if d else d

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
    html.Div(id="status", style={"fontWeight": "bold", "color": "#0f0"}),
    dcc.Interval(id="fast-interval", interval=REFRESH_INTERVAL, n_intervals=0),
    dcc.Store(id="store", data=[]),
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
@callback(
    Output("table", "data"),
    Output("status", "children"),
    Input("fast-interval", "n_intervals"),
    State("table", "data")
)
def update_fast(_, current_data):
    now = datetime.now().strftime("%H:%M:%S")
    live = len(market_state)
    status = f"Live: {live} | {now}"

    # Only update if new data
    if not last_update:
        return current_data, status

    # Build patch
    patch = []
    changed = False
    for sym in underlyings:
        row = get_row(sym)
        if row["_ts"] > (current_data[underlyings.index(sym)]["_ts"] if current_data else 0):
            patch.append(row)
            changed = True

    return patch or dash.no_update, status
