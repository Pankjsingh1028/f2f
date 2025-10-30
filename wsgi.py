from f2fspread_main import app, start_streamer
from datetime import datetime
import threading

def start_ws_once():
    if not getattr(start_ws_once, "started", False):
        print(f"[{datetime.now()}] Starting WebSocket thread...")
        start_streamer()
        start_ws_once.started = True

start_ws_once()
application = app.server
