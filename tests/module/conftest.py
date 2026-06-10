import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select

from core.db import get_db_session
from module.user.model import User


@pytest_asyncio.fixture(loop_scope="session")
async def client():
    from module.api.app import app

    async with LifespanManager(app=app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac


@pytest_asyncio.fixture(loop_scope="session")
async def authenticated_client(client, faker, mock_redis_client):
    from module.api.app import app
    from module.auth import AuthService

    auth_service = app.state.object_registry.get(AuthService)
    auth_service.email_service = AsyncMock()
    auth_service._redis_client = mock_redis_client

    code = "TOKEN"
    auth_service.gen_verification_code = MagicMock(return_value=code)

    username = faker.user_name() + faker.last_name()
    register_payload = {
        "username": username,
        "email": f"{username}@email.com",
        "password": "PAssword1@@1",
    }

    rsp = await client.post("/api/v1/auth/register", json=register_payload)
    assert 200 <= rsp.status_code <= 299

    getdel = mock_redis_client.getdel
    payload = {"action": "verify_email"}
    mock_redis_client.getdel = AsyncMock(return_value=json.dumps(payload))

    # with patch("module.auth.service.get_db_session") as mock_get_db_session:
    #     # Mock db session
    #     mock_db_sess = AsyncMock()

    #     mock_context_manager = AsyncMock()
    #     mock_context_manager.__aenter__.return_value = mock_db_sess
    #     mock_context_manager.__aexit__.return_value = None

    #     mock_get_db_session.return_value = mock_context_manager

    async with get_db_session() as db_sess:
        user = await db_sess.scalar(
            select(User).where(User.username == register_payload["username"])
        )
    
    rsp = await client.post("/api/v1/auth/verify-email", json={"code": code})
    assert 200 <= rsp.status_code <= 299, rsp.json()

    mock_redis_client.getdel = getdel

    yield client
