from unittest.mock import AsyncMock, patch

import pytest
from prometheus_client import REGISTRY, generate_latest

from module.api.middleware.prometheus import PrometheusMiddleware


class TestPrometheusMiddleware:

    @pytest.mark.asyncio
    async def test_non_http_scope_does_not_record_metrics(self):
        with patch("module.api.middleware.prometheus.start_http_server"):
            app = AsyncMock()
            middleware = PrometheusMiddleware(app)
            scope = {"type": "websocket"}

            await middleware(scope, AsyncMock(), AsyncMock())

            app.assert_awaited_once()
            # No http_requests_total metric should exist for this call
            for sample in REGISTRY.collect():
                if sample.name == "http_requests_total":
                    assert len(sample.samples) == 0

    @pytest.mark.asyncio
    async def test_http_request_increments_counter(self):
        with patch("module.api.middleware.prometheus.start_http_server"):
            app = AsyncMock()
            middleware = PrometheusMiddleware(app)
            scope = {"type": "http", "method": "POST", "path": "/orders"}

            await middleware(scope, AsyncMock(), AsyncMock())

            val = REGISTRY.get_sample_value(
                "http_requests_total",
                {"method": "POST", "path": "/orders", "status": "200"},
            )
            assert val == 1.0

    @pytest.mark.asyncio
    async def test_counter_reflects_actual_status_code(self):
        with patch("module.api.middleware.prometheus.start_http_server"):

            async def app_204(scope, receive, send):
                await send({"type": "http.response.start", "status": 204})
                await send({"type": "http.response.body", "body": b""})

            middleware = PrometheusMiddleware(app_204)
            scope = {"type": "http", "method": "DELETE", "path": "/resource/1"}
            await middleware(scope, AsyncMock(), AsyncMock())

            scope = {"type": "http", "method": "PUT", "path": "/resource/2"}
            await middleware(scope, AsyncMock(), AsyncMock())

            val = REGISTRY.get_sample_value(
                "http_requests_total",
                {"method": "DELETE", "path": "/resource/1", "status": "204"},
            )
            assert val == 1.0

    @pytest.mark.asyncio
    async def test_histogram_records_duration(self):
        with patch("module.api.middleware.prometheus.start_http_server"):
            app = AsyncMock()
            middleware = PrometheusMiddleware(app)
            scope = {"type": "http", "method": "GET", "path": "/health"}
            await middleware(scope, AsyncMock(), AsyncMock())

            scope = {"type": "http", "method": "GET", "path": "/resource/1"}
            await middleware(scope, AsyncMock(), AsyncMock())

            count = REGISTRY.get_sample_value(
                "http_request_duration_seconds_count",
                {"method": "GET", "path": "/health"},
            )
            assert count == 1.0
            # A bucket observation and sum should also exist
            assert (
                REGISTRY.get_sample_value(
                    "http_request_duration_seconds_sum",
                    {"method": "GET", "path": "/health"},
                )
                is not None
            )

    @pytest.mark.asyncio
    async def test_generate_latest_includes_metrics(self):
        with patch("module.api.middleware.prometheus.start_http_server"):
            app = AsyncMock()
            middleware = PrometheusMiddleware(app)
            scope = {"type": "http", "method": "PUT", "path": "/update"}
            await middleware(scope, AsyncMock(), AsyncMock())

            scope = {"type": "http", "method": "GET", "path": "/get"}
            await middleware(scope, AsyncMock(), AsyncMock())

            output = generate_latest().decode()
            # Counter
            assert 'http_requests_total{method="PUT",path="/update",status="200"} 1.0' in output
            # Histogram (at least the count line)
            assert 'http_request_duration_seconds_count{method="PUT",path="/update"}' in output
