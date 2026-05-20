import asyncio
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.exc import CustomValidationError
from api.lib.object_registry import ObjectRegistry
from api.middlewares import RateLimitMiddleware
from api.routes.auth.exception import (
    UserAlreadyExistsException,
    UserDoesNotExistException,
)
from api.routes.auth.route import router as auth_router
from api.routes.backtests.exception import (
    InvalidDateRange,
    BacktestNotFoundException,
    BacktestInProgressError,
)
from api.routes.backtests.route import router as backtests_router
from api.routes.backtests.service import APIBacktestsService
from api.routes.broker_connections.exception import (
    BrokerAccountFetchException,
    BrokerConnectionNotFoundException,
    UnsupportedBrokerException,
)
from api.routes.broker_connections.route import router as broker_connections_router
from api.routes.deployments.exception import (
    DeploymentAlreadyRunningException,
    DeploymentNotFoundException,
)
from api.routes.deployments.route import router as deployment_router
from api.routes.deployments.service import APIDeploymentsService
from api.routes.markets.exception import SymbolNotFoundException
from api.routes.markets.service import MarketsService
from api.routes.public.route import router as public_router
from api.routes.strategy.exception import StrategyNotFoundException
from api.routes.strategy.route import router as strategies_router
from api.routes.strategy.service import APIStrategyService
from api.routes.user.route import router as user_router
from api.routes.markets.route import router as markets_router
from config import FRONTEND_DOMAIN, SCHEME, FRONTEND_SUB_DOMAIN
from service.backtest.process import ProcessBacktestService
from service.deployment.consumer import StrategyDeploymentEventsConsumer
from service.deployment.process import ProcessDeploymentService
from service.jwt import JWTError


async def lifespan(app: FastAPI):
    object_registry = ObjectRegistry()
    app.state.object_registry = object_registry

    markets_service = MarketsService()
    object_registry.register(markets_service)

    strategy_service = APIStrategyService()
    object_registry.register(strategy_service)

    backtest_service = ProcessBacktestService()
    object_registry.register(backtest_service)

    api_backtest_service = APIBacktestsService(
        strategy_service=strategy_service,
        backtest_service=backtest_service,
        markets_service=markets_service,
    )
    object_registry.register(api_backtest_service)

    deployment_service = ProcessDeploymentService()
    object_registry.register(deployment_service)

    api_deployments_service = APIDeploymentsService(
        markets_service=MarketsService(), deployment_service=deployment_service
    )
    object_registry.register(api_deployments_service)

    event_consumer = StrategyDeploymentEventsConsumer()
    task = asyncio.create_task(event_consumer.run())
    object_registry.register(event_consumer)

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

app.include_router(auth_router)
app.include_router(backtests_router)
app.include_router(broker_connections_router)
app.include_router(deployment_router)
app.include_router(public_router)
app.include_router(strategies_router)
app.include_router(user_router)
app.include_router(markets_router)


def _error_response(status_code: int, message: str):
    return JSONResponse(status_code=status_code, content={"error": message})


@app.exception_handler(CustomValidationError)
async def handle_custom_validation_error(req: Request, exc: CustomValidationError):
    return _error_response(exc.status_code, exc.msg)


@app.exception_handler(HTTPException)
async def handle_http_exception(req: Request, exc: HTTPException):
    return _error_response(exc.status_code, exc.detail)


@app.exception_handler(JWTError)
async def handle_jwt_error(req: Request, exc: JWTError):
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


@app.exception_handler(BacktestInProgressError)
async def handle_backtest_in_progress_exception(
    req: Request, exc: BacktestInProgressError
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
