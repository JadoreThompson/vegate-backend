from datetime import timedelta

import jwt
from fastapi import Response
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    COOKIE_ALIAS,
    IS_PRODUCTION,
    JWT_ALGO,
    JWT_EXPIRY_SECS,
    JWT_SECRET,
)
from core.db import get_db_session
# from infra.db.model import User
# from utils import get_datetime
from module.user.model import User
from util import get_datetime
from .exception import JWTException
from .schema import JWTPayload


class JWTService:

    def __init__(
        self,
        jwt_secret: str = JWT_SECRET,
        jwt_algo: str = JWT_ALGO,
        jwt_expiry_secs: int = JWT_EXPIRY_SECS,
        cookie_alias: str = COOKIE_ALIAS,
        is_production: bool = IS_PRODUCTION,
    ):
        self._jwt_secret = jwt_secret
        self._jwt_algo = jwt_algo
        self._jwt_expiry_secs = jwt_expiry_secs
        self._cookie_alias = cookie_alias
        self._is_production = is_production

    def _generate_expiry(self) -> int:
        """Private method to generate JWT expiry datetime"""
        return int(
            (get_datetime() + timedelta(seconds=self._jwt_expiry_secs)).timestamp()
        )

    def generate_jwt(self, **kwargs) -> str:
        """Generates a JWT token"""
        if kwargs.get("exp") is None:
            kwargs["exp"] = self._generate_expiry()

        kwargs["sub"] = str(kwargs["sub"])

        payload = JWTPayload(**kwargs)

        return jwt.encode(
            payload.model_dump(mode="json"), self._jwt_secret, algorithm=self._jwt_algo
        )

    def decode_jwt(self, token: str) -> JWTPayload:
        try:
            return JWTPayload(
                **jwt.decode(
                    token,
                    self._jwt_secret,
                    algorithms=[self._jwt_algo],
                )
            )
        except jwt.ExpiredSignatureError:
            raise JWTException("Token has expired")
        except jwt.InvalidTokenError:
            raise JWTException("Invalid token")

    async def set_cookie(
        self, user: User, db_sess: AsyncSession, rsp: Response | None = None
    ) -> Response:
        token = self.generate_jwt(
            sub=user.user_id,
            em=user.email,
            authenticated=user.authenticated_at is not None,
        )

        if rsp is None:
            rsp = Response()

        await db_sess.execute(
            update(User).values(jwt=token).where(User.user_id == user.user_id)
        )

        rsp.set_cookie(
            self._cookie_alias,
            token,
            httponly=True,
            secure=self._is_production,
            expires=self._generate_expiry(),
        )

        return rsp

    def remove_cookie(self, rsp: Response | None = None) -> Response:
        if rsp is None:
            rsp = Response()

        rsp.delete_cookie(
            self._cookie_alias,
            httponly=True,
            secure=self._is_production,
        )

        return rsp

    async def validate_jwt(
        self, token: str, is_authenticated: bool = True
    ) -> JWTPayload:
        """Validate a JWT token and ensure the User exists

        Args:
            token (str): JWT token to validate.
            is_authenticated (bool, optional): Whether or not to check if the user
                is authenticated. Defaults to True.

        Raises:
            JWTException: No user found adhering to the constraints.

        Returns:
            JWTPayload: Original payload
        """
        payload = self.decode_jwt(token)

        if is_authenticated and not payload.authenticated:
            raise JWTException("User not authenticated")

        if payload.exp < int(get_datetime().timestamp()):
            raise JWTException("Expired token")

        async with get_db_session() as db_sess:
            user = await db_sess.scalar(select(User).where(User.user_id == payload.sub))

            if user is None:
                raise JWTException("User not found.")

            if user.jwt is not None and user.jwt != token:
                raise JWTException("Invalid token")

        return payload
