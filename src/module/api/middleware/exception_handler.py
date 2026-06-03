import logging
from typing import Callable

from fastapi import Request, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

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


class GlobalExceptionHandlerMiddleware(BaseHTTPMiddleware):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._handlers: dict[type[Exception], Callable] = {
            HTTPException: self._handle_http_exception,
            JWTException: self._handle_jwt_exception,
            RequestValidationError: self._handle_request_validation_error,
            UserAlreadyExistsException: self._handle_user_already_exists_exception,
            UserDoesNotExistException: self._handle_user_does_not_exist_exception,
            UserNotAuthenticatedException: self._handle_user_not_authenticated_exception,
            InvalidCredentialsException: self._handle_invalid_credentials_exception,
            BrokerAccountFetchException: self._handle_broker_account_fetch_exception,
            UnsupportedBrokerException: self._handle_unsupported_broker_exception,
            StrategyNotFoundException: self._handle_strategy_not_found_exception,
            StrategyVersionNotFoundException: self._handle_strategy_version_not_found_exception,
            VersionForkDetectedException: self._handle_version_fork_detected_exception,
            SymbolNotFoundException: self._handle_symbol_not_found_exception,
            InvalidDateRange: self._handle_invalid_date_range_exception,
            BacktestNotFoundException: self._handle_backtest_not_found_exception,
            BacktestInProgressException: self._handle_backtest_in_progress_exception,
            DeploymentNotFoundException: self._handle_deployment_not_found_exception,
            DeploymentAlreadyRunningException: self._handle_deployment_already_running_exception,
            BrokerConnectionNotFoundException: self._handle_broker_connection_not_found_exception,
            BacktestLimitReached: self._handle_backtest_limit_reached_exception,
            DeploymentLimitReached: self._handle_deployment_limit_reached_exception,
        }
        self._logger = logging.getLogger(self.__class__.__name__)

    def register_handler(self, exc_type: type[Exception], handler: Callable) -> None:
        self._handlers[exc_type] = handler

    async def dispatch(self, request: Request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            handler = self._handlers.get(type(exc))
            if handler is not None:
                return handler(request, exc)
            self._logger.error("An unhandled exception occurred", exc_info=exc)
            return self._create_error_response(
                status_code=500,
                message="An unexpected error occurred. Please try again later.",
            )

    def _create_error_response(self, status_code: int, message: str) -> JSONResponse:
        """Stub for creating a standardized error response."""
        return JSONResponse(status_code=status_code, content={"error": message})

    def _handle_http_exception(self, req: Request, exc: HTTPException):
        return self._create_error_response(exc.status_code, exc.detail)

    def _handle_jwt_exception(self, req: Request, exc: JWTException):
        return self._create_error_response(401, str(exc))

    def _handle_request_validation_error(
        self, req: Request, exc: RequestValidationError
    ):
        error = exc.errors()[0]
        msg = error["msg"]
        error_type = error["type"].replace("_", " ")
        clean_msg = msg.lower().replace(f"{error_type},", "").strip()
        if clean_msg:
            clean_msg = clean_msg[0].upper() + clean_msg[1:]
        return self._create_error_response(422, clean_msg or "Invalid request body")

    def _handle_user_already_exists_exception(
        self, req: Request, exc: UserAlreadyExistsException
    ):
        return self._create_error_response(400, str(exc))

    def _handle_user_does_not_exist_exception(
        self, req: Request, exc: UserDoesNotExistException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_user_not_authenticated_exception(
        self, req: Request, exc: UserNotAuthenticatedException
    ):
        return self._create_error_response(403, str(exc))

    def _handle_invalid_credentials_exception(
        self, req: Request, exc: InvalidCredentialsException
    ):
        return self._create_error_response(422, str(exc))

    def _handle_broker_account_fetch_exception(
        self, req: Request, exc: BrokerAccountFetchException
    ):
        return self._create_error_response(400, str(exc))

    def _handle_unsupported_broker_exception(
        self, req: Request, exc: UnsupportedBrokerException
    ):
        return self._create_error_response(400, str(exc))

    def _handle_strategy_not_found_exception(
        self, req: Request, exc: StrategyNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_strategy_version_not_found_exception(
        self, req: Request, exc: StrategyVersionNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_version_fork_detected_exception(
        self, req: Request, exc: VersionForkDetectedException
    ):
        return self._create_error_response(409, str(exc))

    def _handle_symbol_not_found_exception(
        self, req: Request, exc: SymbolNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_invalid_date_range_exception(self, req: Request, exc: InvalidDateRange):
        return self._create_error_response(400, str(exc))

    def _handle_backtest_not_found_exception(
        self, req: Request, exc: BacktestNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_backtest_in_progress_exception(
        self, req: Request, exc: BacktestInProgressException
    ):
        return self._create_error_response(400, str(exc))

    def _handle_deployment_not_found_exception(
        self, req: Request, exc: DeploymentNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_deployment_already_running_exception(
        self, req: Request, exc: DeploymentAlreadyRunningException
    ):
        return self._create_error_response(400, str(exc))

    def _handle_broker_connection_not_found_exception(
        self, req: Request, exc: BrokerConnectionNotFoundException
    ):
        return self._create_error_response(404, str(exc))

    def _handle_backtest_limit_reached_exception(
        self, req: Request, exc: BacktestLimitReached
    ):
        return self._create_error_response(409, str(exc))

    def _handle_deployment_limit_reached_exception(
        self, req: Request, exc: DeploymentLimitReached
    ):
        return self._create_error_response(409, str(exc))
