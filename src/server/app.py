from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import DOMAIN, SCHEME, SUB_DOMAIN
from server.exc import CustomValidationError, JWTError
from server.middlewares import RateLimitMiddleware
from server.routes.auth.route import router as auth_router
from server.routes.public.route import router as public_router


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"{SCHEME}://{DOMAIN}",
        f"{SCHEME}://{SUB_DOMAIN}{DOMAIN}",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
app.add_middleware(RateLimitMiddleware)

app.include_router(auth_router)
app.include_router(public_router)


@app.exception_handler(CustomValidationError)
async def handle_http_exception(req: Request, exc: CustomValidationError):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.msg})


@app.exception_handler(HTTPException)
async def handle_http_exception(req: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


@app.exception_handler(JWTError)
async def handle_jwt_error(req: Request, exc: JWTError):
    return JSONResponse(status_code=401, content={"error": str(exc)})
