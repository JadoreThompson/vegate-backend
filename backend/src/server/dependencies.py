from fastapi import Request

from config import COOKIE_ALIAS
from server.exc import JWTError
from server.typing import JWTPayload
from server.services import JWTService
from utils.db import smaker


async def depends_db_sess():
    async with smaker.begin() as s:
        try:
            yield s
        except:
            await s.rollback()
            raise


async def depends_jwt(req: Request) -> JWTPayload:
    """Verify the JWT token from the request cookies and validate it.

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

    payload = JWTService.decode_jwt(token)
    return await JWTService.validate_payload(payload)
