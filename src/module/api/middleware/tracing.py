from opentelemetry import trace
from starlette.types import ASGIApp, Scope, Receive, Send

from config import SERVICE_NAME


class TracingMiddleware:

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope.get("path", "")

        tracer = trace.get_tracer(SERVICE_NAME)

        with tracer.start_as_current_span(
            f"{method} {path}",
            attributes={
                "http.method": method,
                "http.url": path,
                "http.route": path,
                "http.host": (scope.get("server") or [None, None])[0] or "",
            },
            kind=trace.SpanKind.SERVER,
        ) as span:
            status_code = 200

            async def send_wrapper(message):
                nonlocal status_code
                if message["type"] == "http.response.start":
                    status_code = message["status"]
                await send(message)

            try:
                await self.app(scope, receive, send_wrapper)
            finally:
                span.set_attribute("http.status_code", status_code)