# gunicorn.conf.py — tuned for Dash live apps

workers = 1                      # Single process so global state (market_state) is shared
worker_class = "eventlet"        # Async worker needed for Dash live updates
worker_connections = 1000
timeout = 60
keepalive = 10
threads = 1                      # eventlet handles concurrency, not threads
bind = "0.0.0.0:8051"
preload_app = False              # Must be False so eventlet patches properly
