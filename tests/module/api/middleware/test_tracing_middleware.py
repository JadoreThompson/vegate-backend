from unittest.mock import AsyncMock

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter

from module.api.middleware.tracing import TracingMiddleware


class _TestSpanExporter(SpanExporter):
    def __init__(self):
        self.spans = []

    def export(self, spans, timeout_millis=30000):
        self.spans.extend(spans)
        return True

    def shutdown(self):
        self.spans.clear()


@pytest.fixture
def tracer():
    exporter = _TestSpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return trace.get_tracer("test", tracer_provider=provider), exporter


class TestTracingMiddleware:

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through(self, tracer):
        tracer_obj, exporter = tracer
        app = AsyncMock()
        middleware = TracingMiddleware(app, tracer=tracer_obj)
        scope = {"type": "websocket"}
        receive = AsyncMock()
        send = AsyncMock()

        await middleware(scope, receive, send)

        app.assert_awaited_once_with(scope, receive, send)
        assert len(exporter.spans) == 0

    @pytest.mark.asyncio
    async def test_http_request_creates_span_with_attributes(self, tracer):
        tracer_obj, exporter = tracer
        app = AsyncMock()
        middleware = TracingMiddleware(app, tracer=tracer_obj)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/test",
            "server": ("localhost", 8000),
        }

        await middleware(scope, AsyncMock(), AsyncMock())

        app.assert_awaited_once()
        assert len(exporter.spans) == 1
        span = exporter.spans[0]
        assert span.name == "GET /test"
        assert span.attributes.get("http.method") == "GET"
        assert span.attributes.get("http.url") == "/test"
        assert span.attributes.get("http.route") == "/test"
        assert span.attributes.get("http.host") == "localhost"
        assert span.attributes.get("http.status_code") == 200
        assert span.kind == trace.SpanKind.SERVER

    @pytest.mark.asyncio
    async def test_span_records_actual_status_code(self, tracer):
        tracer_obj, exporter = tracer

        async def app_with_status(scope, receive, send):
            await send({"type": "http.response.start", "status": 201})
            await send({"type": "http.response.body", "body": b""})

        middleware = TracingMiddleware(app_with_status, tracer=tracer_obj)
        scope = {"type": "http", "method": "POST", "path": "/create"}

        await middleware(scope, AsyncMock(), AsyncMock())

        assert len(exporter.spans) >= 1
        span = exporter.spans[-1]
        assert span.attributes.get("http.status_code") == 201

    @pytest.mark.asyncio
    async def test_no_server_defaults_to_empty_string(self, tracer):
        tracer_obj, exporter = tracer
        app = AsyncMock()
        middleware = TracingMiddleware(app, tracer=tracer_obj)
        scope = {"type": "http", "method": "GET", "path": "/"}

        await middleware(scope, AsyncMock(), AsyncMock())

        assert len(exporter.spans) == 1
        span = exporter.spans[0]
        assert span.attributes.get("http.host") == ""
