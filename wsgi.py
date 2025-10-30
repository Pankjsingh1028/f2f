# wsgi.py
from f2fspread_main import (
    app, load_instruments, load_underlyings, get_instrument_keys_for_symbol,
    initial_rest_poll, start_sdk_streamer
)
from datetime import datetime
import time

print(f"[{datetime.now()}] [GUNICORN] Initializing...")

# Rebuild subscribe_keys safely
instrument_data_list = load_instruments()
underlyings = load_underlyings()

subscribe_keys = []
for sym in underlyings:
    for f in get_instrument_keys_for_symbol(sym, instrument_data_list):
        subscribe_keys.append(f["instrument_key"])
subscribe_keys = list(dict.fromkeys(subscribe_keys))

print(f"[{datetime.now()}] [GUNICORN] Ready: {len(underlyings)} symbols, {len(subscribe_keys)} contracts")

# Start market data
initial_rest_poll(subscribe_keys)
start_sdk_streamer(subscribe_keys)

# Debug
time.sleep(2)
from f2fspread_main import market_state
print(f"[{datetime.now()}] [GUNICORN] market_state populated: {len(market_state)} entries")

application = app.server
