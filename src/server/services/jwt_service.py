from dataclasses import asdict
from datetime import datetime

import jwt
from fastapi import Response
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import COOKIE_ALIAS, IS_PRODUCTION, JWT_SECRET, JWT_ALGO, JWT_EXPIRY
from db_models import Users
from server.exc import JWTError
from server.typing import JWTPayload
from utils.db import get_db_sess, get_datetime


class JWTService:
    @staticmethod
    def generate_jwt(**kwargs) -> str:
        """Generates a JWT token"""
        if kwargs.get("exp") is None:
            kwargs["exp"] = datetime.now() + JWT_EXPIRY
        kwargs["sub"] = str(kwargs["sub"])
        payload = JWTPayload(**kwargs)
        return jwt.encode(asdict(payload), JWT_SECRET, algorithm=JWT_ALGO)

    @staticmethod
    def decode_jwt(token: str) -> JWTPayload:
        try:
            return JWTPayload(**jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO]))
        except jwt.ExpiredSignatureError:
            raise JWTError("Token has expired")
        except jwt.InvalidTokenError:
            raise JWTError("Invalid token")

    @staticmethod
    def set_cookie(user: Users, rsp: Response | None = None) -> Response:
        token = JWTService.generate_jwt(
            sub=user.user_id,
            em=user.email,
            pricing_tier=user.pricing_tier,
            authenticated=user.authenticated_at is not None,
        )
        if rsp is None:
            rsp = Response()

        rsp.set_cookie(
            COOKIE_ALIAS,
            token,
            httponly=True,
            secure=IS_PRODUCTION,
            expires=get_datetime() + JWT_EXPIRY,
        )
        return rsp

    @staticmethod
    async def set_user_cookie(
        user: Users, db_sess: AsyncSession | None = None, rsp: Response | None = None
    ) -> Response:
        token = JWTService.generate_jwt(
            sub=user.user_id,
            em=user.email,
            pricing_tier=user.pricing_tier,
            authenticated=user.authenticated_at is not None,
        )
        if rsp is None:
            rsp = Response()

        if db_sess:
            await db_sess.execute(
                update(Users).values(jwt=token).where(Users.user_id == user.user_id)
            )
        else:
            async with get_db_sess() as db_sess:
                await db_sess.execute(
                    update(Users).values(jwt=token).where(Users.user_id == user.user_id)
                )
            await db_sess.commit()

        rsp.set_cookie(
            COOKIE_ALIAS,
            token,
            httponly=True,
            secure=IS_PRODUCTION,
            expires=get_datetime() + JWT_EXPIRY,
        )
        return rsp

    @staticmethod
    def remove_cookie(rsp: Response | None = None) -> Response:
        if rsp is None:
            rsp = Response()
        rsp.delete_cookie(COOKIE_ALIAS, httponly=True, secure=IS_PRODUCTION)
        return rsp

    @classmethod
    async def validate_jwt(cls, token: str, is_authenticated: bool = True):
        """Validate a JWT token and ensure the Users exists

        Args:
            token (str): JWT token to validate.
            is_authenticated (bool, optional): Whether or not to check if the user
                is authenticated. Defaults to True.

        Raises:
            JWTError: No user found with adhring to the constraints.

        Returns:
            JWTPayload: Original payload
        """
        payload = cls.decode_jwt(token)
        if is_authenticated and not payload.authenticated:
            raise JWTError("User not authenticated")
        if payload.exp < get_datetime().timestamp():
            raise JWTError("Expired token")

        async with get_db_sess() as db_sess:
            user = await db_sess.scalar(
                select(Users).where(Users.user_id == payload.sub)
            )

            if user is None:
                raise JWTError("User not found.")
            if user.jwt is not None and user.jwt != token:
                raise JWTError("Invalid token")

        return payload
