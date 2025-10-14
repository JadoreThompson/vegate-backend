from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.routes.auth.route import router as auth_router


app = FastAPI()

app.include_router(auth_router)

app.add_middleware(
    CORSMiddleware(
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
)
