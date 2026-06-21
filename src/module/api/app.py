import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import (
    EMAIL_SERVICE_NAME,
    EVENT_PUBLISHER_NAME,
    FRONTEND_DOMAIN,
    FRONTEND_SUB_DOMAIN,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_SECONDS,
    SCHEME,
)
from core.db.client import DB_ENGINE, DB_ENGINE_SYNC
from core.redis import REDIS_CLIENT
from core.telemetry import setup_tracing
from module.auth.router import router as auth_router
from module.auth.service import AuthService
from module.backtest import BacktestsService
from module.backtest.router import router as backtests_router
from module.broker_connections import BrokerConnectionsService
from module.broker_connections.router import router as broker_connections_router
from module.contact.router import router as contact_router
from module.deployment import DeploymentsService
from module.deployment.event.broadcast import DeploymentEventBroadcast
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.router import router as deployment_router
from module.email import EmailServiceFactory
from module.event_bus import EventPublisherFactory
from module.jwt import JWTService
from module.markets import MarketsService
from module.markets.router import router as markets_router
from module.strategy import StrategyService
from module.strategy.router import router as strategies_router
from module.user.router import router as user_router
from .middleware import (
    RateLimitMiddleware,
    GlobalExceptionHandlerMiddleware,
    PrometheusMiddleware,
    TracingMiddleware,
)
from .object_registry import ObjectRegistry
from .router import router as api_router


async def lifespan(app: FastAPI):
    setup_tracing(
        fastapi_app=app,
        sqlalchemy_engines=[lambda: DB_ENGINE_SYNC, lambda: DB_ENGINE],
    )
    object_registry = ObjectRegistry()
    app.state.object_registry = object_registry

    event_publisher = EventPublisherFactory.create(EVENT_PUBLISHER_NAME)

    jwt_service = JWTService()
    object_registry.register(jwt_service)

    email_service = EmailServiceFactory.create(
        EMAIL_SERVICE_NAME, "Vegate", "no-reply@vegate.jadore.dev"
    )
    auth_service = AuthService(email_service=email_service)
    object_registry.register(auth_service)

    markets_service = MarketsService()
    object_registry.register(markets_service)

    broker_connections_service = BrokerConnectionsService()
    object_registry.register(broker_connections_service)

    strategy_service = StrategyService(deployment_service=None)
    object_registry.register(strategy_service)

    deployment_service = DeploymentsService(
        strategy_service=strategy_service,
        markets_service=markets_service,
        broker_connections_service=broker_connections_service,
        event_publisher=event_publisher,
    )
    object_registry.register(deployment_service)

    strategy_service._deployment_service = deployment_service

    backtest_service = BacktestsService(
        strategy_service=strategy_service,
        event_publisher=event_publisher,
        markets_service=markets_service,
    )
    object_registry.register(backtest_service)

    deployment_event_broadcast = DeploymentEventBroadcast(
        deserialiser=DeploymentEventDeserialiser()
    )

    tasks: list[asyncio.Task] = []

    tasks.append(asyncio.create_task(deployment_event_broadcast.run()))
    object_registry.register(deployment_event_broadcast)

    yield

    for task in tasks:
        try:
            task.cancel()
            await asyncio.wait_for(task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    await object_registry.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(TracingMiddleware)
app.add_middleware(GlobalExceptionHandlerMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"{SCHEME}://{FRONTEND_DOMAIN}",
        f"{SCHEME}://{FRONTEND_SUB_DOMAIN}{FRONTEND_DOMAIN}",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
app.add_middleware(
    RateLimitMiddleware,
    redis_client=REDIS_CLIENT,
    limit=RATE_LIMIT_REQUESTS,
    window=RATE_LIMIT_SECONDS,
)
app.add_middleware(PrometheusMiddleware)

app.include_router(api_router)
app.include_router(auth_router)
app.include_router(backtests_router)
app.include_router(broker_connections_router)
app.include_router(contact_router)
app.include_router(deployment_router)
app.include_router(strategies_router)
app.include_router(user_router)
app.include_router(markets_router)
