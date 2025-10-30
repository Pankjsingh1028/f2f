# wsgi.py
from f2fspread_main import app, subscribe_keys, initial_rest_poll, start_sdk_streamer
from datetime import datetime

# Start WebSocket in the worker process
print(f"[{datetime.now()}] [Gunicorn Worker] Starting WebSocket...")
initial_rest_poll(subscribe_keys)
start_sdk_streamer(subscribe_keys)

application = app.server
