from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.exc import CustomValidationError
from api.middlewares import RateLimitMiddleware
from api.routes.auth.route import router as auth_router
from api.routes.backtests.route import router as backtests_router
from api.routes.brokers.route import router as brokers_router
from api.routes.brokers.alpaca.route import router as broker_alpaca_router
from api.routes.deployments.route import router as deployment_router
# from api.routes.markets.route import router as market_router
from api.routes.public.route import router as public_router
from api.routes.strategies.route import router as strategies_router
from config import FRONTEND_DOMAIN, SCHEME, FRONTEND_SUB_DOMAIN
from services.jwt import JWTError


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
app.include_router(brokers_router)
app.include_router(broker_alpaca_router)
app.include_router(deployment_router)
# app.include_router(market_router)
app.include_router(public_router)
app.include_router(strategies_router)


@app.exception_handler(CustomValidationError)
async def handle_http_exception(req: Request, exc: CustomValidationError):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.msg})


@app.exception_handler(HTTPException)
async def handle_http_exception(req: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


@app.exception_handler(JWTError)
async def handle_jwt_error(req: Request, exc: JWTError):
    return JSONResponse(status_code=401, content={"error": str(exc)})
