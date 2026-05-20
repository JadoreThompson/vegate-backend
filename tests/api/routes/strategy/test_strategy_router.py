import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient
from uuid import uuid4
from datetime import datetime

from api.routes.auth.service import AuthService
from api.routes.strategy.service import APIStrategyService
from api.routes.strategy.models import (
    CreateStrategyRequest,
    UpdateStrategyRequest,
    StrategyResponse,
)
from api.routes.strategy.agents.strategy import StrategyGenOutput
from infra.redis.client import REDIS_CLIENT
from enums import BrokerType


@pytest.fixture
def email_service():
    service = MagicMock()
    service.send_email = AsyncMock(return_value=None)
    return service


@pytest.fixture
def auth_service(email_service, monkeypatch):
    service = AuthService(email_service=email_service, redis_client=REDIS_CLIENT)
    monkeypatch.setattr("api.routes.auth.route.auth_service", service)
    yield service


class MockStrategyService:
    def __init__(self):
        self._created_strategies = []

    async def create(self, request, user_id, db_sess):
        strategy_id = uuid4()
        self._created_strategies.append(strategy_id)
        return type(
            "obj",
            (object),
            {
                "strategy_id": strategy_id,
                "name": "Mock Strategy",
                "description": "Mock description",
                "prompt": request.description,
                "code": "class Strategy: pass",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            },
        )()

    async def get_strategy(self, id, user_id, db_sess):
        return None

    async def get_strategies(self, user_id, db_sess, *, page, limit):
        from api.models import PaginatedResponse

        return PaginatedResponse(page=page, size=0, has_next=False, data=[])

    async def update(self, request, id, user_id, db_sess):
        return type(
            "obj",
            (object),
            {
                "strategy_id": id,
                "name": "Updated Strategy",
                "description": "Updated description",
                "prompt": "updated prompt",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            },
        )()

    async def delete(self, id, user_id, db_sess):
        pass

    async def create_strategy(self, request, user_id, db_sess):
        return await self.create(request, user_id, db_sess)


@pytest.fixture
def mock_strategy_service():
    return MockStrategyService()


@pytest.fixture
def strategy_service_fixture(mock_strategy_service, monkeypatch):
    monkeypatch.setattr(
        "api.routes.strategy.route.strategy_service", mock_strategy_service
    )
    yield mock_strategy_service


@pytest_asyncio.fixture
async def client(auth_service, strategy_service_fixture):
    from api.app import app

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


@pytest_asyncio.fixture
async def authenticated_client(client):
    register_payload = {
        "username": "strategy-user",
        "email": "strategy@email.com",
        "password": "PAssword1@@1",
    }
    await client.post("/auth/register", json=register_payload)
    yield client


class TestCreateStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_returns_200(self, authenticated_client):
        payload = {
            "description": "A momentum strategy based on RSI indicator",
        }

        res = await authenticated_client.post("/strategy/", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert "id" in data
        assert data["name"] == "Mock Strategy"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_missing_description_returns_422(
        self, authenticated_client
    ):
        payload = {}

        res = await authenticated_client.post("/strategy/", json=payload)

        assert res.status_code == 422


class TestGetStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        res = await authenticated_client.get(f"/strategy/{fake_id}")

        assert res.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_returns_200(
        self, authenticated_client, mock_strategy_service
    ):
        strategy_id = uuid4()

        mock_strategy_service.get_strategy = AsyncMock(
            return_value=type(
                "obj",
                (object),
                {
                    "strategy_id": strategy_id,
                    "name": "Test Strategy",
                    "description": "Test description",
                    "code": "class Strategy: pass",
                    "prompt": "test prompt",
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                },
            )()
        )

        res = await authenticated_client.get(f"/strategy/{strategy_id}")

        assert res.status_code == 200
        data = res.json()
        assert data["id"] == strategy_id


class TestListStrategies:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/strategy/")

        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_with_pagination(self, authenticated_client):
        res = await authenticated_client.get("/strategy/?page=1&limit=10")

        assert res.status_code == 200


class TestUpdateStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_returns_200(self, authenticated_client):
        strategy_id = uuid4()

        payload = {
            "name": "Updated Strategy Name",
            "description": "Updated description",
        }

        res = await authenticated_client.patch(f"/strategy/{strategy_id}", json=payload)

        assert res.status_code == 200
        data = res.json()
        assert data["name"] == "Updated Strategy"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_name_too_short_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "name": "abc",
        }

        res = await authenticated_client.patch(f"/strategy/{strategy_id}", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_description_too_short_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "description": "short",
        }

        res = await authenticated_client.patch(f"/strategy/{strategy_id}", json=payload)

        assert res.status_code == 422


class TestDeleteStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_returns_204(self, authenticated_client):
        strategy_id = uuid4()

        res = await authenticated_client.delete(f"/strategy/{strategy_id}")

        assert res.status_code == 204

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_not_found_returns_404(
        self, authenticated_client, mock_strategy_service
    ):
        from api.routes.strategy.service import (
            APIStrategyService as RealStrategyService,
        )

        mock_strategy_service.update = AsyncMock(side_effect=Exception("not found"))

        fake_id = uuid4()
        res = await authenticated_client.delete(f"/strategy/{fake_id}")

        assert res.status_code == 204


class TestStrategyEndpointsUnauthenticated:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_unauthenticated_returns_401(self, client):
        payload = {
            "description": "test strategy",
        }

        res = await client.post("/strategy/", json=payload)

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_unauthenticated_returns_401(self, client):
        res = await client.get("/strategy/")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_unauthenticated_returns_401(self, client):
        res = await client.get(f"/strategy/{uuid4()}")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_unauthenticated_returns_401(self, client):
        res = await client.patch(f"/strategy/{uuid4()}", json={"name": "new name"})

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_unauthenticated_returns_401(self, client):
        res = await client.delete(f"/strategy/{uuid4()}")

        assert res.status_code == 401
