import pytest
import pytest_asyncio

from sqlalchemy import delete

from core.db.session import get_db_sess_sync
from module.user.model import User


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(User))
        db_sess.commit()


class TestGetMe:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_me_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/api/v1/users/me")

        assert res.status_code == 200
        data = res.json()
        assert "username" in data
        assert isinstance(data["username"], str)
        assert len(data["username"]) > 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_me_returns_401_without_auth(self, client):
        res = await client.get("/api/v1/users/me")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_me_returns_401_with_invalid_token(self, client):
        client.cookies.set("vegate-cookie", "invalid-token")

        res = await client.get("/api/v1/users/me")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_me_returns_401_with_nonexistent_user(self, client, faker):
        from module.api.app import app
        from module.jwt import JWTService

        jwt_service = app.state.object_registry.get(JWTService)

        token = jwt_service.generate_jwt(
            sub="00000000-0000-0000-0000-000000000000",
            em="nonexistent@email.com",
            authenticated=True,
        )
        client.cookies.set("vegate-cookie", token)

        res = await client.get("/api/v1/users/me")

        assert res.status_code == 401
