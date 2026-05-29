import asyncio

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import (
    BACKTEST_EXECUTOR_NAME,
    FRONTEND_DOMAIN,
    FRONTEND_SUB_DOMAIN,
    MAX_CONCURRENT_BACKTESTS,
    MAX_CONCURRENT_DEPLOYMENTS,
    SCHEME,
)
from module.auth.exception import (
    InvalidCredentialsException,
    UserAlreadyExistsException,
    UserDoesNotExistException,
    UserNotAuthenticatedException,
)
from module.auth.router import router as auth_router
from module.auth.service import AuthService
from module.backtest import BacktestsService
from module.backtest.exception import (
    BacktestInProgressException,
    BacktestNotFoundException,
    InvalidDateRange,
)
from module.backtest.executor import BacktestExecutorFactory
from module.backtest.executor.exception import BacktestLimitReached
from module.backtest.router import router as backtests_router
from module.broker_connections import BrokerConnectionsService
from module.broker_connections.exception import (
    BrokerAccountFetchException,
    BrokerConnectionNotFoundException,
    UnsupportedBrokerException,
)
from module.broker_connections.router import router as broker_connections_router
from module.contact.router import router as contact_router
from module.deployment import DeploymentsService
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.event.relay import DeploymentEventRelay
from module.deployment.exception import (
    DeploymentAlreadyRunningException,
    DeploymentNotFoundException,
)
from module.deployment.executor import DeploymentExecutorFactory
from module.deployment.router import router as deployment_router
from module.email import BrevoEmailService, SmtpgoEmailService
from module.jwt import JWTService, JWTException
from module.markets import MarketsService
from module.markets.exception import SymbolNotFoundException
from module.markets.router import router as markets_router
from module.strategy import StrategyService
from module.strategy.exception import (
    StrategyNotFoundException,
    StrategyVersionNotFoundException,
    VersionForkDetectedException,
)
from module.strategy.router import router as strategies_router
from module.user.router import router as user_router
from .middleware import RateLimitMiddleware
from .router import router as api_router
from .object_registry import ObjectRegistry


async def lifespan(app: FastAPI):
    object_registry = ObjectRegistry()
    app.state.object_registry = object_registry

    jwt_service = JWTService()
    object_registry.register(jwt_service)

    auth_service = AuthService(email_service_cls=BrevoEmailService)
    # auth_service = AuthService(email_service_cls=SmtpgoEmailService)
    object_registry.register(auth_service)

    markets_service = MarketsService()
    object_registry.register(markets_service)

    broker_connections_service = BrokerConnectionsService()
    object_registry.register(broker_connections_service)

    strategy_service = StrategyService()
    object_registry.register(strategy_service)

    backtest_executor = BacktestExecutorFactory.create(BACKTEST_EXECUTOR_NAME)
    backtest_executor.max_concurrent_backtests = MAX_CONCURRENT_BACKTESTS
    backtest_service = BacktestsService(
        strategy_service=strategy_service,
        backtest_executor=backtest_executor,
        markets_service=markets_service,
    )
    object_registry.register(backtest_service)

    broker_connections_service = BrokerConnectionsService()
    object_registry.register(backtest_service)

    deployment_executor = DeploymentExecutorFactory.create(BACKTEST_EXECUTOR_NAME)
    deployment_executor.max_concurrent_deployments = MAX_CONCURRENT_DEPLOYMENTS
    deployment_service = DeploymentsService(
        markets_service=markets_service,
        deployment_executor=deployment_executor,
        broker_connections_service=broker_connections_service,
    )
    object_registry.register(deployment_service)

    event_relay = DeploymentEventRelay(deserialiser=DeploymentEventDeserialiser())
    task = asyncio.create_task(event_relay.run())
    object_registry.register(event_relay)

    yield

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await object_registry.close()


app = FastAPI(lifespan=lifespan)

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
app.add_middleware(RateLimitMiddleware)

app.include_router(api_router)
app.include_router(auth_router)
app.include_router(backtests_router)
app.include_router(broker_connections_router)
app.include_router(contact_router)
app.include_router(deployment_router)
app.include_router(strategies_router)
app.include_router(user_router)
app.include_router(markets_router)


def _error_response(status_code: int, message: str):
    return JSONResponse(status_code=status_code, content={"error": message})


@app.exception_handler(HTTPException)
async def handle_http_exception(req: Request, exc: HTTPException):
    return _error_response(exc.status_code, exc.detail)


@app.exception_handler(JWTException)
async def handle_jwt_error(req: Request, exc: JWTException):
    return _error_response(401, str(exc))


@app.exception_handler(Exception)
async def handle_exception(req: Request, exc: Exception):
    return _error_response(
        500,
        "An unexpected error occurred. Please try again later.",
    )


@app.exception_handler(RequestValidationError)
async def handle_request_validation_error(req: Request, exc: RequestValidationError):
    error = exc.errors()[0]

    msg = error["msg"]
    error_type = error["type"].replace("_", " ")

    clean_msg = msg.lower().replace(f"{error_type},", "").strip()

    if clean_msg:
        clean_msg = clean_msg[0].upper() + clean_msg[1:]

    return _error_response(422, clean_msg or "Invalid request body")


@app.exception_handler(UserAlreadyExistsException)
async def handle_user_already_exists_exception(
    req: Request, exc: UserAlreadyExistsException
):
    return _error_response(400, str(exc))


@app.exception_handler(UserDoesNotExistException)
async def handle_user_already_exists_exception(
    req: Request, exc: UserDoesNotExistException
):
    return _error_response(404, str(exc))


@app.exception_handler(UserNotAuthenticatedException)
async def handle_user_not_authenticated_exception(
    req: Request, exc: UserNotAuthenticatedException
):
    return _error_response(403, str(exc))


@app.exception_handler(InvalidCredentialsException)
async def handle_invalid_credentials_exception(
    req: Request, exc: InvalidCredentialsException
):
    return _error_response(422, str(exc))


@app.exception_handler(BrokerAccountFetchException)
async def handle_broker_account_fetch_exception(
    req: Request, exc: BrokerAccountFetchException
):
    return _error_response(400, str(exc))


@app.exception_handler(UnsupportedBrokerException)
async def handle_broker_connection_exception(
    req: Request, exc: UnsupportedBrokerException
):
    return _error_response(400, str(exc))


@app.exception_handler(StrategyNotFoundException)
async def handle_strategy_not_found_exception(
    req: Request, exc: StrategyNotFoundException
):
    return _error_response(404, str(exc))


@app.exception_handler(StrategyVersionNotFoundException)
async def handle_strategy_version_not_found_exception(
    req: Request, exc: StrategyVersionNotFoundException
):
    return _error_response(404, str(exc))


@app.exception_handler(VersionForkDetectedException)
async def handle_version_fork_detected_exception(
    req: Request, exc: VersionForkDetectedException
):
    return _error_response(409, str(exc))


@app.exception_handler(SymbolNotFoundException)
async def handle_symbol_not_found_exception(req: Request, exc: SymbolNotFoundException):
    return _error_response(404, str(exc))


@app.exception_handler(InvalidDateRange)
async def handle_invalid_date_range_exception(req: Request, exc: InvalidDateRange):
    return _error_response(400, str(exc))


@app.exception_handler(BacktestNotFoundException)
async def handle_backtest_not_found_exception(
    req: Request, exc: BacktestNotFoundException
):
    return _error_response(404, str(exc))


@app.exception_handler(BacktestInProgressException)
async def handle_backtest_in_progress_exception(
    req: Request, exc: BacktestInProgressException
):
    return _error_response(400, str(exc))


@app.exception_handler(DeploymentNotFoundException)
async def handle_backtest_not_found_exception(
    req: Request, exc: DeploymentNotFoundException
):
    return _error_response(404, str(exc))


@app.exception_handler(DeploymentAlreadyRunningException)
async def handle_deployment_already_running_exception(
    req: Request, exc: DeploymentAlreadyRunningException
):
    return _error_response(400, str(exc))


@app.exception_handler(BrokerConnectionNotFoundException)
async def handle_backtest_not_found_exception(
    req: Request, exc: BrokerConnectionNotFoundException
):
    return _error_response(404, str(exc))


@app.exception_handler(BacktestLimitReached)
async def handle_backtest_limit_reached_exception(
    req: Request, exc: BacktestLimitReached
):
    return _error_response(400, str(exc))
