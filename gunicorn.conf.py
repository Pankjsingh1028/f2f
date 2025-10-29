# gunicorn.conf.py
workers = 2                      # Two workers for smoother multi-viewer performance
threads = 4                      # Each worker handles 4 threads
worker_class = "gthread"         # Threaded worker class (more stable for Dash callbacks)
timeout = 60                     # Give time for slow network/dash callbacks
keepalive = 10
bind = "0.0.0.0:8051"
preload_app = True               # Loads app once before workers fork (saves resources)
