from datetime import datetime, timedelta

import jwt
from fastapi import Response
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import COOKIE_ALIAS, IS_PRODUCTION, JWT_SECRET, JWT_ALGO, JWT_EXPIRY_SECS
from infra.db import get_db_sess
from infra.db.models import Users
from utils import get_datetime
from .exc import JWTError
from .models import JWTPayload


class JWTService:
    @classmethod
    def _generate_expiry(cls) -> datetime:
        """Private method to generate JWT expiry datetime"""
        return int((get_datetime() + timedelta(seconds=JWT_EXPIRY_SECS)).timestamp())

    @classmethod
    def generate_jwt(cls, **kwargs) -> str:
        """Generates a JWT token"""
        if kwargs.get("exp") is None:
            kwargs["exp"] = cls._generate_expiry()
        kwargs["sub"] = str(kwargs["sub"])
        payload = JWTPayload(**kwargs)
        return jwt.encode(
            payload.model_dump(mode="json"), JWT_SECRET, algorithm=JWT_ALGO
        )

    @classmethod
    def decode_jwt(cls, token: str) -> JWTPayload:
        try:
            return JWTPayload(**jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO]))
        except jwt.ExpiredSignatureError:
            raise JWTError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise JWTError("Invalid token")

    @classmethod
    def set_cookie(cls, user: Users, rsp: Response | None = None) -> Response:
        token = cls.generate_jwt(
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
            expires=cls._generate_expiry(),
        )
        return rsp

    @classmethod
    async def set_user_cookie(
        cls,
        user: Users,
        db_sess: AsyncSession | None = None,
        rsp: Response | None = None,
    ) -> Response:
        token = cls.generate_jwt(
            sub=user.user_id,
            em=user.email,
            pricing_tier=user.pricing_tier,
            authenticated=user.authenticated_at is not None,
        )
        if rsp is None:
            rsp = Response()

        await db_sess.execute(
            update(Users).values(jwt=token).where(Users.user_id == user.user_id)
        )

        rsp.set_cookie(
            COOKIE_ALIAS,
            token,
            httponly=True,
            secure=IS_PRODUCTION,
            expires=cls._generate_expiry(),
        )
        return rsp

    @classmethod
    def remove_cookie(cls, rsp: Response | None = None) -> Response:
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
        if payload.exp < int(get_datetime().timestamp()):
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
