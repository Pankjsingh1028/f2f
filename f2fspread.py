import os
import json
import time
import threading
import queue  # Not used now, but kept for compatibility
import pandas as pd
import requests # Added for REST API polling
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
# Changed refresh interval from 5000ms to 1000ms for faster, more responsive updates 
# from the WebSocket data thread.
REFRESH_INTERVAL = 2000  # ms 
MARKET_QUOTE_URL = 'https://api.upstox.com/v2/market-quote/quotes' # Base URL for quotes

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

# ---------------- DATA LOADING AND PREPARATION ----------------

def load_instruments():
    """Loads all instruments from the JSON file into a single list."""
    try:
        with open(INSTRUMENTS_JSON, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {INSTRUMENTS_JSON} not found.")
        return []

def load_instruments_to_dict(instruments_list):
    """
    Transforms the instruments list into a dictionary keyed by instrument_key 
    for O(1) average time complexity lookup, greatly improving performance 
    over list iteration.
    """
    instrument_map = {}
    for instrument in instruments_list:
        ik = instrument.get("instrument_key")
        if ik:
            instrument_map[ik] = instrument
    return instrument_map


def load_underlyings():
    """Loads the list of underlying symbols from the CSV file."""
    try:
        df = pd.read_csv(STOCKS_CSV)
        return df["underlying_symbol"].dropna().unique().tolist()
    except FileNotFoundError:
        print(f"Error: {STOCKS_CSV} not found.")
        return []

def get_instrument_keys_for_symbol(symbol, instrument_data_list):
    """Return the 3 nearest FUT contracts for the symbol, using correct Upstox keys."""
    futures = []
    for instrument in instrument_data_list:
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


# ---------------- SDK CALLBACK (UNCHANGED) ----------------
def on_message(message):
    """SDK callback: Parse live_feed and update market_state."""
    if message.get("type") != "live_feed":
        return

    feeds = message.get("feeds", {})
    for ik, payload in feeds.items():
        full_feed = payload.get("fullFeed", {})
        market_ff = full_feed.get("marketFF", {})

        # --- Extract LTP ---
        ltpc = market_ff.get("ltpc", {})
        ltp = safe_float(ltpc.get("ltp"))

        # --- Extract Best Bid/Ask from Depth ---
        depth_list = market_ff.get("marketLevel", {}).get("bidAskQuote", [])
        bid = safe_float(depth_list[0].get("bidP")) if depth_list else None
        ask = safe_float(depth_list[0].get("askP")) if depth_list else None

        with market_state_lock:
            if ltp is not None or bid is not None or ask is not None:
                pass  # Suppress log
            market_state[ik] = {
                "bidP": bid,
                "askP": ask,
                "ltp": ltp,
                "ts": datetime.now(timezone.utc).isoformat()
            }

def on_open():
    print(f"[{datetime.now()}] SDK WS Connected & Subscribed")

def on_error(error):
    print(f"[{datetime.now()}] SDK Error: {error}")

def on_reconnecting():
    print(f"[{datetime.now()}] SDK Reconnecting...")

# ---------------- REST API POLLING ----------------

def initial_rest_poll(subscribe_keys):
    """
    Polls the Upstox Market Quote API for initial ask/bid data for all instruments.
    This runs once at startup before the real-time stream takes over.
    """
    print(f"[{datetime.now()}] Initializing state by polling {len(subscribe_keys)} instruments...")
    
    # API supports max ~490 keys per request. Using 490 for efficient batching.
    batch_size = 490 # Updated batch_size from 50 to 490 based on user feedback.
    for i in range(0, len(subscribe_keys), batch_size):
        keys_batch = subscribe_keys[i:i + batch_size]
        instrument_keys_str = ",".join(keys_batch)
        
        url = f"{MARKET_QUOTE_URL}?instrument_key={instrument_keys_str}"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {ACCESS_TOKEN}'
        }

        try:
            # Implement simple exponential backoff for robustness
            max_retries = 3
            delay = 1
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                    
                    data = response.json().get("data", {})

                    with market_state_lock:
                        for ik_long, quote in data.items():
                            depth = quote.get("depth", {})
                            buy_depth = depth.get("buy", [])
                            sell_depth = depth.get("sell", [])

                            bid = safe_float(buy_depth[0].get("price")) if buy_depth else None
                            ask = safe_float(sell_depth[0].get("price")) if sell_depth else None

                            # REST uses "last_price", not "ltp"
                            ltp = safe_float(quote.get("last_price"))

                            ik = quote.get("instrument_token") or ik_long

                            if bid is not None or ask is not None or ltp is not None:
                                market_state[ik] = {
                                    "bidP": bid,
                                    "askP": ask,
                                    "ltp": ltp,
                                    "ts": datetime.now(timezone.utc).isoformat()
                                }    
                                
                    print(f"[{datetime.now()}] Successfully polled batch {i//batch_size + 1} (Instruments: {len(keys_batch)})")
                    break # Success, break out of retry loop
                
                except requests.exceptions.HTTPError as e:
                    # Specific handling for 401 Unauthorized
                    if e.response.status_code == 401:
                        print(f"[{datetime.now()}] **FATAL ERROR:** 401 Client Error: Unauthorized. Please check your ACCESS_TOKEN in the .env file.")
                        # Do not retry 401 errors
                        raise # Re-raise the exception to stop further execution if token is bad
                    
                    # Handle other HTTP errors with retry
                    if attempt < max_retries - 1:
                        print(f"[{datetime.now()}] HTTP Error ({e.response.status_code}) on batch starting at index {i}. Retrying in {delay}s...")
                        time.sleep(delay)
                        delay *= 2 # Exponential backoff
                    else:
                        print(f"[{datetime.now()}] Final HTTP Error ({e.response.status_code}) during initial REST poll for batch starting at index {i}: {e}")
                        
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        print(f"[{datetime.now()}] Request timed out for batch starting at index {i}. Retrying in {delay}s...")
                        time.sleep(delay)
                        delay *= 2 # Exponential backoff
                    else:
                        print(f"[{datetime.now()}] Final Timeout during initial REST poll for batch starting at index {i}.")
                        
                except requests.exceptions.RequestException as e:
                    print(f"[{datetime.now()}] Error during initial REST poll for batch starting at index {i}: {e}")
                    break # Non-recoverable request error (e.g., DNS, connection refused)
                except Exception as e:
                    print(f"[{datetime.now()}] An unexpected error occurred in polling: {e}")
                    break # General unexpected error
        except Exception as e:
            # Catch the re-raised 401 error or other critical failure
            print(f"[{datetime.now()}] Initial polling interrupted due to critical error.")
            break 
        
        # Add a small delay between batches
        time.sleep(0.1)
    
    print(f"[{datetime.now()}] Initial polling complete.")

# ---------------- MARGIN FETCHING (NEW) ----------------
def fetch_margin_for_spread(near_key, next_key, lot_size):
    """
    Fetch combined margin for Near BUY + Next SELL (1 lot each)
    using Upstox Margin API. Respects rate limits (<=50/sec).
    """
    url = "https://api.upstox.com/v2/charges/margin"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    data = {
        "instruments": [
            {
                "instrument_key": near_key,
                "quantity": lot_size,
                "transaction_type": "BUY",
                "product": "D"
            },
            {
                "instrument_key": next_key,
                "quantity": lot_size,
                "transaction_type": "SELL",
                "product": "D"
            }
        ]
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        if resp.status_code == 200:
            js = resp.json()
            if js.get("status") == "success":
                return js["data"].get("final_margin") or js["data"].get("required_margin")
        else:
            print(f"[Margin] HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[Margin] Error fetching margin: {e}")
    return None

# ---------------- BROKERAGE CHARGES (SPREAD BOTH DIRECTIONS) ----------------
def fetch_spread_charges(near_key, next_key, lot_size, near_price, next_price):
    """
    Fetch total brokerage + taxes for:
      1. Near BUY + Next SELL
      2. Near SELL + Next BUY
    Returns a tuple: (charges_nearBuy_nextSell, charges_nearSell_nextBuy)
    """
    url = "https://api.upstox.com/v2/charges/brokerage"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    def get_total(instrument_key, qty, txn, price):
        params = {
            "instrument_token": instrument_key,
            "quantity": qty,
            "product": "D",
            "transaction_type": txn,
            "price": price or 0
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if j.get("status") == "success":
                    return j["data"]["charges"].get("total")
            else:
                print(f"[Charges] HTTP {r.status_code}: {r.text[:150]}")
        except Exception as e:
            print(f"[Charges] Error: {e}")
        return None

    # 1️⃣ Near BUY + Next SELL
    c1_buy = get_total(near_key, lot_size, "BUY", near_price)
    time.sleep(0.05)  # tiny delay to stay <50 req/s
    c1_sell = get_total(next_key, lot_size, "SELL", next_price)
    c_nearBuy_nextSell = (c1_buy or 0) + (c1_sell or 0)

    # 2️⃣ Near SELL + Next BUY
    c2_sell = get_total(near_key, lot_size, "SELL", near_price)
    time.sleep(0.05)
    c2_buy = get_total(next_key, lot_size, "BUY", next_price)
    c_nearSell_nextBuy = (c2_sell or 0) + (c2_buy or 0)

    return c_nearBuy_nextSell, c_nearSell_nextBuy

def calculate_cost_of_carry_from_margin(margin, expiry_timestamp, roi=12.0):
    """
    Calculate cost of carry using margin amount instead of notional value.
    margin: capital blocked for the spread (in ₹)
    expiry_timestamp: milliseconds since epoch
    roi: annual interest rate (default 12%)
    """
    if not margin or not expiry_timestamp:
        return None
    try:
        expiry_date = datetime.fromtimestamp(expiry_timestamp / 1000)
        today = datetime.now()
        days_left = max((expiry_date - today).days, 0)
        cost = margin * (roi / 100) * (days_left / 365)
        return round(cost, 2)
    except Exception as e:
        print(f"[Carry] Error calculating carry from margin: {e}")
        return None

# ---------------- SDK SETUP (UNCHANGED) ----------------
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

# ---------------- CORE DASHBOARD LOGIC (Updated to use new map) ----------------

def get_state(ik, tsym):
    with market_state_lock:
        s = market_state.get(ik, {})
        return {
            "symbol": tsym,
            "bidP": s.get("bidP"),
            "askP": s.get("askP"),
            "ltp": s.get("ltp")
        }

def compute_spreads(near, nxt, far):
    near_ltp = safe_float(near.get("ltp"))
    if not near_ltp or near_ltp == 0:
        near_ltp = 1  # avoid divide by zero

    def pct(a, b):
        d = diff(a, b)
        return round((d / near_ltp) * 100, 2) if d is not None else None

    # absolute spreads (in points)
    abs_spreads = {
        "Spread_NearBuy_NextSell": diff(nxt.get("bidP"), near.get("askP")),
        "Spread_NearBuy_FarSell": diff(far.get("bidP"), near.get("askP")),
        "Spread_NextBuy_FarSell": diff(far.get("bidP"), nxt.get("askP")),
        "Spread_NearSell_NextBuy": diff(near.get("bidP"), nxt.get("askP")),
        "Spread_NearSell_FarBuy": diff(near.get("bidP"), far.get("askP")),
        "Spread_NextSell_FarBuy": diff(nxt.get("bidP"), far.get("askP")),
    }

    # percentage spreads
    pct_spreads = {
        k + "_pct": pct(nxt.get("bidP") if "Next" in k else far.get("bidP"), near.get("askP") if "Buy" in k else near.get("bidP"))
        if "Near" in k else pct(far.get("bidP"), nxt.get("askP"))
        for k in abs_spreads.keys()
    }

    return {**abs_spreads, **pct_spreads}

def build_df(underlyings, instrument_data_list):
    rows = []
    for sym in underlyings:
        # Use the list-based function for expiry sorting, as map won't work easily here
        futs = get_instrument_keys_for_symbol(sym, instrument_data_list) 
        
        near, nxt, far = (futs + [None]*3)[:3]
        
        # Safe state fetching
        near_s = get_state(near["instrument_key"], near["trading_symbol"]) if near and near.get("instrument_key") else {}
        nxt_s = get_state(nxt["instrument_key"], nxt["trading_symbol"]) if nxt and nxt.get("instrument_key") else {}
        far_s = get_state(far["instrument_key"], far["trading_symbol"]) if far and far.get("instrument_key") else {}
        
        # Ensure symbols are displayed even if market data is missing
        near_sym = near_s.get("symbol") or (near["trading_symbol"] if near else None)
        nxt_sym = nxt_s.get("symbol") or (nxt["trading_symbol"] if nxt else None)
        far_sym = far_s.get("symbol") or (far["trading_symbol"] if far else None)
        
        spreads = compute_spreads(near_s, nxt_s, far_s)

        # Find lot size from near future instrument metadata
        lot_size = None
        if near and near.get("instrument_key"):
            for inst in instrument_data_list:
                if inst.get("instrument_key") == near["instrument_key"]:
                    lot_size = inst.get("lot_size")
                    break

        margin_val = margin_cache.get(sym)

        carry_cost = None
        if near and near.get("expiry"):
            margin_val = margin_cache.get(sym)
            carry_cost = calculate_cost_of_carry_from_margin(margin_val, near.get("expiry"))
        
        rows.append({
            "Symbol": sym,
            "Lot_Size": lot_size,
            "Margin": round(margin_val, 2) if margin_val is not None else None,
            "Charges_f": charges_forward_cache.get(sym),
            "Charges_r": charges_reverse_cache.get(sym),
            #"Charges": round((charges_forward_cache.get(sym)+charges_reverse_cache.get(sym)),2),
            "Charges": (round((charges_forward_cache.get(sym) or 0) + (charges_reverse_cache.get(sym) or 0), 2)),
            "Cost_of_Carry": round(carry_cost or 0, 2),
            #"Near": near_sym,
            "Near_ltp": near_s.get("ltp"),
            "Near_bid": near_s.get("bidP"),
            "Near_ask": near_s.get("askP"),
            #"Next": nxt_sym,
            "Next_ltp": nxt_s.get("ltp"),
            "Next_bid": nxt_s.get("bidP"),
            "Next_ask": nxt_s.get("askP"),
            #"Far": far_sym,
            "Far_ltp": far_s.get("ltp"),
            "Far_bid": far_s.get("bidP"),
            "Far_ask": far_s.get("askP"),
            **spreads,
        })
    return pd.DataFrame(rows)

# ---------------- DASH APP INITIALIZATION ----------------
app = dash.Dash(__name__)
app.title = "Futures Spread Dashboard"

# 1. Load instruments once into memory (list structure used for initial key extraction)
instrument_data_list = load_instruments()

# Optional: Load into dictionary for faster lookups in future enhancements
# instrument_data_map = load_instruments_to_dict(instrument_data_list) 

# 2. Load underlyings and get subscription keys
underlyings = load_underlyings()

subscribe_keys = []
for s in underlyings:
    for f in get_instrument_keys_for_symbol(s, instrument_data_list):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))

# 3. Perform initial REST poll to populate state once
initial_rest_poll(subscribe_keys)

# 3B. Fetch margin & both-direction charges once (startup cache)
print(f"[{datetime.now()}] Fetching margin + charges for all underlyings...")

margin_cache = {}
charges_forward_cache = {}   # Near BUY + Next SELL
charges_reverse_cache = {}   # Near SELL + Next BUY

for sym in underlyings:
    futs = get_instrument_keys_for_symbol(sym, instrument_data_list)
    if len(futs) < 2:
        continue

    near, nxt = futs[:2]
    lot_size = next((inst.get("lot_size") for inst in instrument_data_list
                     if inst.get("instrument_key") == near["instrument_key"]), 1)

    # --- Margin for Near BUY + Next SELL ---
    margin_val = fetch_margin_for_spread(near["instrument_key"], nxt["instrument_key"], lot_size)
    margin_cache[sym] = margin_val

    # --- Get prices (from market_state if available) ---
    with market_state_lock:
        near_price = None
        next_price = None
        if near["instrument_key"] in market_state:
            near_price = market_state[near["instrument_key"]].get("ltp") or market_state[near["instrument_key"]].get("askP")
        if nxt["instrument_key"] in market_state:
            next_price = market_state[nxt["instrument_key"]].get("ltp") or market_state[nxt["instrument_key"]].get("bidP")

    # --- Charges for both spread directions ---
    c_forward, c_reverse = fetch_spread_charges(
        near["instrument_key"], nxt["instrument_key"], lot_size, near_price, next_price
    )
    charges_forward_cache[sym] = c_forward
    charges_reverse_cache[sym] = c_reverse

    print(f"[{sym}] Margin: {margin_val} | FwdCharges: {c_forward} | RevCharges: {c_reverse}")
    time.sleep(0.1)  # ~10 requests/sec total

# 4. Start the real-time stream
print(f"Subscribing to {len(subscribe_keys)} instruments via SDK")
start_sdk_streamer(subscribe_keys)


# ---------------- DASH LAYOUT AND CALLBACKS (UNCHANGED) ----------------
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
            #{"name": "Charges_f", "id": "Charges_f", "type": "numeric"},
            #{"name": "Charges_r", "id": "Charges_r", "type": "numeric"},

            {"name": "Near LTP", "id": "Near_ltp"},
            {"name": "Next LTP", "id": "Next_ltp"},
            {"name": "Far LTP", "id": "Far_ltp"},

            # --- Absolute Spreads (points) ---
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
                "Spread_NextBuy_FarSell", "Spread_NearSell_NextBuy",
                "Spread_NearSell_FarBuy", "Spread_NextSell_FarBuy"
            ]
        ] + [
            {
                "if": {"filter_query": f"{{{col}}} < 0", "column_id": col},
                "backgroundColor": "#5a0a0a",
                "color": "white"
            } for col in [
                "Spread_NearBuy_NextSell",
                "Spread_NearBuy_FarSell",
                "Spread_NextBuy_FarSell",
                "Spread_NearSell_NextBuy",
                "Spread_NearSell_FarBuy",
                "Spread_NextSell_FarBuy"
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
    # Pass the list structure because get_instrument_keys_for_symbol needs to iterate and sort.
    df = build_df(underlyings, instrument_data_list) 
    now = datetime.now().strftime("%H:%M:%S")
    return df.to_dict("records"), f"Last updated: {now}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)
