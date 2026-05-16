from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.exc import CustomValidationError
from api.middlewares import RateLimitMiddleware
from api.routes.auth.exception import (
    UserAlreadyExistsException,
    UserDoesNotExistException,
)
from api.routes.auth.route import router as auth_router
from api.routes.backtests.route import router as backtests_router
from api.routes.broker_connections.exception import BrokerAccountFetchException, UnsupportedBrokerException
from api.routes.broker_connections.route import  router as broker_connections_router
from api.routes.deployments.route import router as deployment_router
from api.routes.public.route import router as public_router
from api.routes.strategies.route import router as strategies_router
from config import FRONTEND_DOMAIN, SCHEME, FRONTEND_SUB_DOMAIN
from service.jwt import JWTError

app = FastAPI()

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
