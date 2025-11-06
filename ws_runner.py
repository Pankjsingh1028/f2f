import time
import threading
from f2fspread_main import bootstrap_market_data, snapshot_state_to_file

if __name__ == "__main__":
    print("[ws_runner] Starting WebSocket ingestion...")

    bootstrap_market_data()

    def _snap_loop():
        while True:
            snapshot_state_to_file()
            time.sleep(0.5)

    threading.Thread(target=_snap_loop, daemon=True).start()

    while True:
        time.sleep(5)

