from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio
from httpx import AsyncClient, ASGITransport


@pytest_asyncio.fixture(loop_scope="session")
async def client():
    from api.app import app

    async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest_asyncio.fixture(loop_scope="session")
async def authenticated_client(client, faker):
    from api.routes.auth.route import auth_service

    auth_service._email_service = AsyncMock()

    code = "TOKEN"
    auth_service.gen_verification_code = MagicMock(return_value=code)

    username = faker.user_name()
    register_payload = {
        "username": username,
        "email": f"{username}@email.com",
        "password": "PAssword1@@1",
    }
    rsp = await client.post("/auth/register", json=register_payload)
    assert 200 <= rsp.status_code <= 299

    rsp = await client.post("/auth/verify-email", json={"code": code})
    assert 200 <= rsp.status_code <= 299

    yield client
