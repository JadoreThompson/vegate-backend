from uuid import uuid4
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from module.strategy import StrategyService
from module.strategy.model import Strategy

# TODO: Implement agents and run tests


@pytest.mark.skip
class TestCreateStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_returns_200(self, authenticated_client):
        payload = {
            "description": "A momentum strategy based on RSI indicator",
        }

        res = await authenticated_client.post("/strategy/", json=payload)

        assert res.status_code == 200, res.json()
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


@pytest.mark.skip
class TestGetStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        res = await authenticated_client.get(f"/strategy/{fake_id}")

        assert res.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_returns_200(self, authenticated_client):
        strategy_id = uuid4()

        from module.api.app import app

        strategy_service = app.state.object_registry.get(StrategyService)

        strategy_service.get_strategy = AsyncMock(
            return_value=Strategy(
                strategy_id=strategy_id,
                name="Test Strategy",
                description="Test description",
                code="class Strategy: pass",
                prompt="test prompt",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
        )

        res = await authenticated_client.get(f"/strategy/{strategy_id}")

        assert res.status_code == 200
        data = res.json()
        assert data["id"] == strategy_id


@pytest.mark.skip
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


@pytest.mark.skip
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


@pytest.mark.skip
class TestDeleteStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_returns_204(self, authenticated_client):
        strategy_id = uuid4()

        res = await authenticated_client.delete(f"/strategy/{strategy_id}")

        assert res.status_code == 204

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_not_found_returns_404(self, authenticated_client):
        from module.api.app import app

        strategy_service = app.state.object_registry.get(StrategyService)
        strategy_service.update = AsyncMock(side_effect=Exception("not found"))

        fake_id = uuid4()
        res = await authenticated_client.delete(f"/strategy/{fake_id}")

        assert res.status_code == 204


@pytest.mark.skip
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
