import logging
from typing import Callable

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from module.auth.exception import (
    EmailAlreadyVerifiedException,
    InvalidCredentialsException,
    InvalidVerificationCodeException,
    UserAlreadyExistsException,
    UserNotFoundExcpetion,
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
    DeploymentExistsException,
    StrategyNotFoundException,
    StrategyVersionNotFoundException,
    VersionForkDetectedException,
)


class GlobalExceptionHandlerMiddleware(BaseHTTPMiddleware):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._handlers: dict[type[Exception], Callable] = {
            # Custom handlers
            HTTPException: self._handle_http_exception,
            RequestValidationError: self._handle_request_validation_error,
            # 400
            UserAlreadyExistsException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            BrokerAccountFetchException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            UnsupportedBrokerException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            InvalidDateRange: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            BacktestInProgressException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            DeploymentAlreadyRunningException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            DeploymentExistsException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            InvalidVerificationCodeException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            EmailAlreadyVerifiedException: lambda req, exc: self._create_error_response(
                400, str(exc)
            ),
            # 401
            JWTException: lambda req, exc: self._create_error_response(401, str(exc)),
            # 403
            UserNotAuthenticatedException: lambda req, exc: self._create_error_response(
                403, str(exc)
            ),
            # 404
            UserNotFoundExcpetion: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            StrategyNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            StrategyVersionNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            SymbolNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            BacktestNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            DeploymentNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            BrokerConnectionNotFoundException: lambda req, exc: self._create_error_response(
                404, str(exc)
            ),
            # 409
            VersionForkDetectedException: lambda req, exc: self._create_error_response(
                409, str(exc)
            ),
            BacktestLimitReached: lambda req, exc: self._create_error_response(
                409, str(exc)
            ),
            DeploymentLimitReached: lambda req, exc: self._create_error_response(
                409, str(exc)
            ),
            # 422
            InvalidCredentialsException: lambda req, exc: self._create_error_response(
                422, str(exc)
            ),
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
        return JSONResponse(
            status_code=status_code,
            content={"error": message},
        )

    def _handle_http_exception(
        self,
        req: Request,
        exc: HTTPException,
    ) -> JSONResponse:
        return self._create_error_response(exc.status_code, exc.detail)

    def _handle_request_validation_error(
        self,
        req: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        error = exc.errors()[0]

        msg = error["msg"]
        error_type = error["type"].replace("_", " ")

        clean_msg = msg.lower().replace(f"{error_type},", "").strip()

        if clean_msg:
            clean_msg = clean_msg[0].upper() + clean_msg[1:]

        return self._create_error_response(422, clean_msg or "Invalid request body")
