import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from module.api.middleware.exception_handler import GlobalExceptionHandlerMiddleware
from module.auth.exception import (
    InvalidCredentialsException,
    UserAlreadyExistsException,
    UserDoesNotExistException,
    UserNotAuthenticatedException,
)
from module.backtest.exception import (
    BacktestInProgressException,
    BacktestNotFoundException,
    InvalidDateRange,
)
from module.backtest.executor.exception import BacktestLimitReached
from module.broker.enums import BrokerType
from module.broker_connections.exception import (
    BrokerAccountFetchException,
    BrokerConnectionNotFoundException,
    UnsupportedBrokerException,
)
from module.deployment.exception import (
    DeploymentAlreadyRunningException,
    DeploymentNotFoundException,
)
from module.deployment.executor.exception import DeploymentLimitReached
from module.jwt import JWTException
from module.markets.exception import SymbolNotFoundException
from module.strategy.exception import (
    StrategyNotFoundException,
    StrategyVersionNotFoundException,
    VersionForkDetectedException,
)


@pytest.fixture
def middleware():
    return GlobalExceptionHandlerMiddleware(AsyncMock())


@pytest.fixture
def mock_request():
    return MagicMock(spec=Request)


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_passthrough(middleware, mock_request):
    call_next = AsyncMock(return_value=JSONResponse({"ok": True}, status_code=200))
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 200
    assert json.loads(response.body) == {"ok": True}


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_http_exception(middleware, mock_request):
    exc = HTTPException(status_code=403, detail="Forbidden")
    call_next = AsyncMock(side_effect=exc)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 403
    assert json.loads(response.body) == {"error": "Forbidden"}


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_jwt_exception(middleware, mock_request):
    exc = JWTException("token expired")
    call_next = AsyncMock(side_effect=exc)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 401
    assert json.loads(response.body) == {"error": "token expired"}


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_request_validation_error(middleware, mock_request):
    exc = RequestValidationError(
        [{"msg": "field required", "type": "value_error.missing"}]
    )
    call_next = AsyncMock(side_effect=exc)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 422
    assert json.loads(response.body) == {"error": "Field required"}


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_request_validation_error_fallback(middleware, mock_request):
    exc = RequestValidationError(
        [{"msg": "value error.missing, ", "type": "value_error.missing"}]
    )
    call_next = AsyncMock(side_effect=exc)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 422
    assert json.loads(response.body) == {"error": "Invalid request body"}


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatch_unhandled_exception(middleware, mock_request):
    exc = RuntimeError("Throwing side effect exception")
    call_next = AsyncMock(side_effect=exc)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == 500
    assert json.loads(response.body) == {
        "error": "An unexpected error occurred. Please try again later."
    }


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    ("exception", "expected_status"),
    [
        (UserAlreadyExistsException(), 400),
        (UserDoesNotExistException(), 404),
        (UserNotAuthenticatedException(), 403),
        (InvalidCredentialsException(), 422),
        (BacktestNotFoundException("test-id"), 404),
        (BacktestInProgressException(), 400),
        (InvalidDateRange("invalid range"), 400),
        (BacktestLimitReached(), 409),
        (BrokerAccountFetchException(), 400),
        (BrokerConnectionNotFoundException(), 404),
        (UnsupportedBrokerException(BrokerType.ALPACA), 400),
        (DeploymentNotFoundException(uuid4()), 404),
        (DeploymentAlreadyRunningException(uuid4()), 400),
        (DeploymentLimitReached(), 409),
        (JWTException("custom msg"), 401),
        (SymbolNotFoundException("AAPL"), 404),
        (StrategyNotFoundException(), 404),
        (StrategyVersionNotFoundException(), 404),
        (VersionForkDetectedException(), 409),
    ],
)
async def test_dispatch_known_exceptions(
    middleware, mock_request, exception, expected_status
):
    call_next = AsyncMock(side_effect=exception)
    response = await middleware.dispatch(mock_request, call_next)
    assert response.status_code == expected_status
    assert json.loads(response.body) == {"error": str(exception)}


def test_register_handler(middleware):
    handler = MagicMock()
    middleware.register_handler(ValueError, handler)
    assert middleware._handlers[ValueError] is handler
