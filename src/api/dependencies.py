from typing import Any, Callable, Type, TypeVar

from fastapi import Depends, Request

from api.lib.object_registry import ObjectRegistry
from api.types import JWTPayload
from config import COOKIE_ALIAS
from infra.db import smaker
from service.deployment.base import DeploymentService
from service.deployment.process import ProcessDeploymentService
from service.jwt import JWTService, JWTError

T = TypeVar("T")
DEFAULT = object()
deployment_service = ProcessDeploymentService()


async def depends_db_sess():
    async with smaker.begin() as s:
        try:
            yield s
        except:
            await s.rollback()
            raise


def depends_jwt(is_authenticated: bool = True):
    """Verify the JWT token from the request cookies and validate it."""

    async def func(req: Request) -> JWTPayload:
        """
        Args:
            req (Request)

        Raises:
            JWTError: If the JWT token is missing, expired, or invalid.

        Returns:
            JWTPayload: The decoded JWT payload if valid.
        """
        token = req.cookies.get(COOKIE_ALIAS)

        if not token:
            raise JWTError("Authentication token is missing")

        return await JWTService.validate_jwt(token, is_authenticated=is_authenticated)

    return func


def CSVQuery(
    name: str, Typ: Type[T], default=DEFAULT, default_factory: Callable[[], Any] = list
):
    def func(req: Request) -> list[T]:
        vals = req.query_params.get(name)
        if name is None or vals is None:
            if default != DEFAULT:
                return default
            if default_factory:
                return default_factory()

        return [Typ(val.strip()) for val in vals.strip().split(",")]

    return Depends(func)


def depends_deployment_service() -> DeploymentService:
    return deployment_service


def depends_class(typ: Type):
    def _func(req: Request):
        object_registry: ObjectRegistry = req.app.state.object_registry
        return object_registry.get(typ)
    return _func
