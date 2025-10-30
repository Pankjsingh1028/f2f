# wsgi.py
from f2fspread_main import app, start_streamer
from datetime import datetime

print(f"[{datetime.now()}] Starting WebSocket...")
start_streamer()
application = app.server
