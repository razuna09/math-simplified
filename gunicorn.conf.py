import os


bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"

# gevent prevents one long-lived SSE request from blocking a sync worker.
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "gevent")
workers = int(os.getenv("GUNICORN_WORKERS", "3"))
worker_connections = int(os.getenv("GUNICORN_WORKER_CONNECTIONS", "1000"))

timeout = int(os.getenv("GUNICORN_TIMEOUT", "180"))
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", "30"))
keepalive = int(os.getenv("GUNICORN_KEEPALIVE", "75"))

accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
