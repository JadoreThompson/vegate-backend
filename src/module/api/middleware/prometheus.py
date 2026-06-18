import time

from prometheus_client import start_http_server, Counter, Histogram
from starlette.types import ASGIApp, Scope, Receive, Send

from config import PROMETHEUS_SERVER_PORT

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    labelnames=["method", "path", "status"],
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


class PrometheusMiddleware:

    def __init__(self, app: ASGIApp):
        self.app = app
        start_http_server(PROMETHEUS_SERVER_PORT)

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope.get("path", "")

        start = time.perf_counter()
        status_code = 200

        async def send_wrapper(message):
            nonlocal status_code

            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            REQUEST_COUNT.labels(method=method, path=path, status=status_code).inc()
            REQUEST_DURATION.labels(method=method, path=path).observe(duration)
