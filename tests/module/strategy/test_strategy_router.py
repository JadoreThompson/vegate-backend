from uuid import uuid4
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from module.strategy import StrategyService
from module.strategy.model import Strategy

# TODO: Implement agents and run tests


class TestCreateStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_returns_200(self, authenticated_client):
        payload = {
            "name": "Mock Strategy",
            "description": "A momentum strategy based on RSI indicator",
        }

        res = await authenticated_client.post("/api/v1/strategy/", json=payload)

        assert res.status_code == 200, res.json()
        data = res.json()
        assert "id" in data
        assert data["name"] == "Mock Strategy"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_missing_description_returns_200(
        self, authenticated_client
    ):
        payload = {"name": "Missing description"}

        res = await authenticated_client.post("/api/v1/strategy/", json=payload)

        assert res.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_missing_name_returns_422(self, authenticated_client):
        payload = {"description": "Missing name"}

        res = await authenticated_client.post("/api/v1/strategy/", json=payload)

        assert res.status_code == 422


class TestGetStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        res = await authenticated_client.get(f"/api/v1/strategy/{fake_id}")

        assert res.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_returns_200(self, authenticated_client):
        strategy_id = uuid4()

        from module.api.app import app

        strategy_service = app.state.object_registry.get(StrategyService)

        strategy_service.get_strategy = AsyncMock(
            return_value=Strategy(
                id=strategy_id,
                name="Test Strategy",
                description="Test description",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
        )

        res = await authenticated_client.get(f"/api/v1/strategy/{strategy_id}")

        assert res.status_code == 200
        data = res.json()
        assert data["id"] == str(strategy_id)


class TestListStrategies:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/api/v1/strategy/")

        assert res.status_code == 200
        data = res.json()
        assert "page" in data
        assert "size" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_with_pagination(self, authenticated_client):
        res = await authenticated_client.get("/api/v1/strategy/?page=1&limit=10")

        assert res.status_code == 200


class TestUpdateStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_returns_200(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Old Name"}
        )
        strategy_id = strat_res.json()["id"]

        payload = {
            "name": "New Name Here",
            "description": "Updated description",
        }

        res = await authenticated_client.patch(
            f"/api/v1/strategy/{strategy_id}", json=payload
        )

        assert res.status_code == 200
        data = res.json()
        assert data["name"] == "New Name Here"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_name_too_short_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "name": "abc",
        }

        res = await authenticated_client.patch(
            f"/api/v1/strategy/{strategy_id}", json=payload
        )

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_description_too_short_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "name": "What a nice strategy",
            "description": "short",
        }

        res = await authenticated_client.patch(
            f"/api/v1/strategy/{strategy_id}", json=payload
        )

        assert res.status_code == 422


class TestDeleteStrategy:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_returns_204(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "To Delete"}
        )
        strategy_id = strat_res.json()["id"]

        res = await authenticated_client.delete(f"/api/v1/strategy/{strategy_id}")

        assert res.status_code == 204

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        res = await authenticated_client.delete(f"/api/v1/strategy/{fake_id}")

        assert res.status_code == 404


class TestStrategyEndpointsUnauthenticated:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_strategy_unauthenticated_returns_401(self, client):
        payload = {
            "name": "test strategy",
        }

        res = await client.post("/api/v1/strategy/", json=payload)

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_strategies_unauthenticated_returns_401(self, client):
        res = await client.get("/api/v1/strategy/")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_strategy_unauthenticated_returns_401(self, client):
        res = await client.get(f"/api/v1/strategy/{uuid4()}")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_update_strategy_unauthenticated_returns_401(self, client):
        res = await client.patch(
            f"/api/v1/strategy/{uuid4()}", json={"name": "new name"}
        )

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_strategy_unauthenticated_returns_401(self, client):
        res = await client.delete(f"/api/v1/strategy/{uuid4()}")

        assert res.status_code == 401


class TestCreateVersion:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_version_returns_201(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Version Test"}
        )
        data = strat_res.json()
        strategy_id = data["id"]
        cur_version_id = data["cur_version_id"]

        res = await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={"prev_version_id": cur_version_id, "code": "class Strategy: pass"},
        )
        assert res.status_code == 201
        data = res.json()
        assert data["code"] == "class Strategy: pass"
        assert data["strategy_id"] == strategy_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_version_sets_prev_version(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Prev Ver"}
        )
        data = strat_res.json()
        strategy_id = data["id"]
        cur_version_id = data["cur_version_id"]

        v1 = await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={"prev_version_id": cur_version_id, "code": "v1"},
        )
        v1_id = v1.json()["id"]

        v2 = await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={"prev_version_id": v1_id, "code": "v2"},
        )
        assert v2.status_code == 201
        assert v2.json()["prev_version"] == v1_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_version_non_existent_prev_version_returns_404(
        self, authenticated_client
    ):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Non existent Test"}
        )
        data = strat_res.json()
        strategy_id = data["id"]

        res = await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={
                "prev_version_id": "00000000-0000-0000-0000-000000000000",
                "code": "Non existent forked",
            },
        )
        assert res.status_code == 404


class TestListVersions:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_versions_returns_paginated(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "List Ver"}
        )
        strategy_id = strat_res.json()["id"]
        list_data = strat_res.json()
        await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={"prev_version_id": list_data["cur_version_id"], "code": "code"},
        )

        res = await authenticated_client.get(f"/api/v1/strategy/{strategy_id}/versions")
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 1
        assert data["size"] >= 1


class TestGetVersion:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_version_returns_200(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Get Ver"}
        )
        strategy_id = strat_res.json()["id"]
        ver_res = await authenticated_client.post(
            f"/api/v1/strategy/{strategy_id}/versions",
            json={
                "prev_version_id": strat_res.json()["cur_version_id"],
                "code": "code",
            },
        )
        version_id = ver_res.json()["id"]

        res = await authenticated_client.get(
            f"/api/v1/strategy/{strategy_id}/versions/{version_id}"
        )
        assert res.status_code == 200
        assert res.json()["id"] == version_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_version_not_found_returns_404(self, authenticated_client):
        strat_res = await authenticated_client.post(
            "/api/v1/strategy/", json={"name": "Get Ver NF"}
        )
        strategy_id = strat_res.json()["id"]

        res = await authenticated_client.get(
            f"/api/v1/strategy/{strategy_id}/versions/{uuid4()}"
        )
        assert res.status_code == 404


class TestVersionEndpointsUnauthenticated:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_versions_unauthenticated_returns_401(self, client):
        res = await client.get(f"/api/v1/strategy/{uuid4()}/versions")
        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_version_unauthenticated_returns_401(self, client):
        res = await client.post(
            f"/api/v1/strategy/{uuid4()}/versions",
            json={"code": "test"},
        )
        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_version_unauthenticated_returns_401(self, client):
        res = await client.get(f"/api/v1/strategy/{uuid4()}/versions/{uuid4()}")
        assert res.status_code == 401
