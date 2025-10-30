# wsgi.py
import os
from datetime import datetime
import time

# Import ONLY the app and functions
from f2fspread_main import (
    app, 
    load_instruments, load_underlyings, 
    get_instrument_keys_for_symbol,
    initial_rest_poll, start_sdk_streamer
)

print(f"[{datetime.now()}] [GUNICORN] Loading static data...")

# === REBUILD subscribe_keys HERE ===
instrument_data_list = load_instruments()
underlyings = load_underlyings()

subscribe_keys = []
for sym in underlyings:
    for f in get_instrument_keys_for_symbol(sym, instrument_data_list):
        subscribe_keys.append(str(f["instrument_key"]))  # str() for safety
subscribe_keys = list(dict.fromkeys(subscribe_keys))

print(f"[{datetime.now()}] [GUNICORN] Loaded {len(underlyings)} symbols, {len(subscribe_keys)} contracts")

# === NOW start WebSocket ===
print(f"[{datetime.now()}] [GUNICORN] Starting initial poll and WebSocket...")
initial_rest_poll(subscribe_keys)
start_sdk_streamer(subscribe_keys)

# Optional: wait for first data
time.sleep(2)
from f2fspread_main import market_state
print(f"[{datetime.now()}] [GUNICORN] market_state has {len(market_state)} entries after startup")

application = app.server
