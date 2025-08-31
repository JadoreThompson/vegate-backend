import aiohttp
from fastapi import Request

from config import COOKIE_ALIAS
from utils import smaker
from server.exc import JWTError
from server.typing import JWTPayload
from server.services import JWTService


async def depends_http_sess():
    async with aiohttp.ClientSession() as sess:
        yield sess


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

    payload = JWTService.decode(token)
    return await JWTService.validate_payload(payload)
