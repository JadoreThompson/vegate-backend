import jwt

from dataclasses import asdict
from datetime import datetime
from fastapi import Response
from sqlalchemy import select

from config import COOKIE_ALIAS, PRODUCTION, JWT_SECRET, JWT_ALGO, JWT_EXPIRY
from db_models import Users
from server.typing import JWTPayload
from utils import get_datetime, get_db_sess
from ..exc import JWTError


class JWTService:
    @staticmethod
    def generate(**kwargs) -> str:
        """Generates a JWT token"""
        if kwargs.get("exp") is None:
            kwargs["exp"] = datetime.now() + JWT_EXPIRY
        kwargs["sub"] = str(kwargs["sub"])
        payload = JWTPayload(**kwargs)
        return jwt.encode(asdict(payload), JWT_SECRET, algorithm=JWT_ALGO)

    @staticmethod
    def decode(token: str) -> JWTPayload:
        try:
            return JWTPayload(**jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO]))
        except jwt.ExpiredSignatureError:
            raise JWTError("Token has expired")
        except jwt.InvalidTokenError:
            raise JWTError("Invalid token")

    @staticmethod
    def set_cookie(user: Users, rsp: Response | None = None) -> Response:
        token = JWTService.generate(sub=user.user_id)
        if rsp is None:
            rsp = Response()

        rsp.set_cookie(
            COOKIE_ALIAS,
            token,
            httponly=True,
            secure=PRODUCTION,
            expires=get_datetime() + JWT_EXPIRY,
        )
        return rsp

    @staticmethod
    def remove_cookie(rsp: Response | None = None) -> Response:
        if rsp is None:
            rsp = Response()
        rsp.delete_cookie(COOKIE_ALIAS, httponly=True, secure=PRODUCTION)
        return rsp

    @staticmethod
    async def validate_payload(payload: JWTPayload) -> JWTPayload:
        """Validate a JWT payload and ensure the Users exists"""
        async with get_db_sess() as sess:
            user = await sess.scalar(select(Users).where(Users.user_id == payload.sub))

        if not user:
            raise JWTError("Invalid user.")
        return payload
