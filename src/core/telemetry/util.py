from __future__ import annotations

import logging
from typing import Callable

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

from config import SERVICE_NAME, TEMPO_BASE_URL
from .export import SimpleConsoleExporter

_logger = logging.getLogger(__name__)


def setup_tracing(
    fastapi_app: FastAPI | None = None,
    sqlalchemy_engines: list[Callable[[], object]] | None = None,
) -> TracerProvider | None:
    """Initialise OpenTelemetry tracing and optionally instrument
    FastAPI, SQLAlchemy, aiohttp client, Redis, and Kafka.

    Must be called exactly once at application startup.
    Returns ``None`` when ``TEMPO_BASE_URL`` is not set (tracing disabled).
    """
    if TEMPO_BASE_URL is not None and TEMPO_BASE_URL.strip():
        _logger.warning("TEMPO_BASE_URL is None. Aborting setup")
        return
    
    resource = Resource.create(attributes={"service.name": SERVICE_NAME})

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{TEMPO_BASE_URL}/v1/traces"))
    )
    provider.add_span_processor(SimpleSpanProcessor(SimpleConsoleExporter()))
    trace.set_tracer_provider(provider)

    if fastapi_app is not None:
        FastAPIInstrumentor.instrument_app(fastapi_app, tracer_provider=provider)
        _logger.info("FastAPI instrumented — traces to %s/v1/traces", TEMPO_BASE_URL)

    if sqlalchemy_engines:
        for engine_factory in sqlalchemy_engines:
            try:
                engine = engine_factory()
                if engine is not None:
                    SQLAlchemyInstrumentor().instrument(engine=engine)
                    _logger.info("SQLAlchemy instrumented")
            except Exception as exc:
                _logger.warning("Failed to instrument SQLAlchemy: %s", exc)

    try:
        AioHttpClientInstrumentor().instrument()
        _logger.info("aiohttp client instrumented")
    except Exception as exc:
        _logger.warning("Failed to instrument aiohttp client: %s", exc)

    try:
        RedisInstrumentor().instrument()
        _logger.info("Redis instrumented")
    except Exception as exc:
        _logger.warning("Failed to instrument Redis: %s", exc)

    try:
        KafkaInstrumentor().instrument()
        _logger.info("Kafka instrumented")
    except Exception as exc:
        _logger.warning("Failed to instrument Kafka: %s", exc)

    return provider
