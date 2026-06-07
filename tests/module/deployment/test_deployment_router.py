import asyncio
import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from module.deployment import DeploymentsService
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event import (
    DeploymentEventType,
    DeploymentStatusChangedEvent,
)
from module.deployment.event.relay import DeploymentEventRelay
from module.deployment.model import DeploymentEvent, StrategyDeployments
from module.jwt.schema import JWTPayload
from module.strategy import StrategyService
from module.strategy.model import Strategy
from module.strategy.schema import StrategyResponse
from module.strategy.agents.strategy_gen import StrategyGenOutput
from module.util import seed_candles
from core.db import get_db_sess_sync, get_db_session
from module.broker_connections.model import BrokerConnections
from vegate.oms.enums import BrokerType


async def create_strategy(
    client, prompt: str = "Create a test strategy"
) -> StrategyResponse:
    from module.api.app import app

    object_registry = app.state.object_registry
    strategy_service = object_registry.get(StrategyService)

    rsp = await client.post(
        "/api/v1/strategy/",
        json={"name": "Deployment router tests strategy", "description": prompt},
    )
    rsp.raise_for_status()
    data = StrategyResponse(**rsp.json())

    return data


async def create_broker_connection(client) -> str:
    from module.api.app import app
    from module.api.object_registry import ObjectRegistry
    from module.broker_connections import BrokerConnectionsService
    from module.broker_connections.service import _BrokerAccount

    object_registry: ObjectRegistry = app.state.object_registry
    broker_connections_service = object_registry.get(BrokerConnectionsService)

    broker_connections_service._fetch_alpaca_account_id = AsyncMock(
        return_value=_BrokerAccount(id="mock-account-id", number="mock-account-number")
    )

    payload = {
        "broker": "alpaca",
        "api_key": "test-api-key",
        "secret_key": "test-secret-key",
    }

    rsp = await client.post("/api/v1/broker-connections", json=payload)
    rsp.raise_for_status()
    return rsp.json()["id"]


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(StrategyDeployments))
        db_sess.execute(delete(BrokerConnections))
        db_sess.commit()


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed():
    seed_candles()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


def _mock_deployment_runner():
    from module.api.app import app

    deployments_service: DeploymentsService = app.state.object_registry.get(
        DeploymentsService
    )
    deployments_service._event_publisher.publish = AsyncMock()


def _mock_relay():
    from module.api.app import app
    from module.deployment.event.relay import DeploymentEventRelay

    relay = app.state.object_registry.get(DeploymentEventRelay)
    relay.register = AsyncMock()
    relay.remove = AsyncMock()
    return relay


class TestCreateDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_deployment_returns_201(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)

        assert rsp.status_code == 201, rsp.json()
        data = rsp.json()
        assert "id" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_deployment_missing_version_id_returns_422(
        self, authenticated_client
    ):
        payload = {
            "broker_connection_id": str(uuid4()),
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)

        assert rsp.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_deployment_missing_broker_connection_id_returns_422(
        self, authenticated_client
    ):
        payload = {
            "version_id": str(uuid4()),
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)

        assert rsp.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_deployment_unauthenticated_returns_401(self, client):
        payload = {
            "version_id": str(uuid4()),
            "broker_connection_id": str(uuid4()),
        }

        rsp = await client.post("/api/v1/deployments/", json=payload)

        assert rsp.status_code == 401


class TestGetDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_deployment_returns_200(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        rsp = await authenticated_client.get(f"/api/v1/deployments/{deployment_id}")

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert "id" in data
        assert "status" in data
        assert "version_id" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_deployment_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        rsp = await authenticated_client.get(f"/api/v1/deployments/{fake_id}")

        assert rsp.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_deployment_unauthenticated_returns_401(self, client):
        rsp = await client.get(f"/api/v1/deployments/{uuid4()}")

        assert rsp.status_code == 401


class TestListDeployments:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_deployments_returns_200(self, authenticated_client):
        rsp = await authenticated_client.get("/api/v1/deployments/")

        assert rsp.status_code == 200
        data = rsp.json()
        assert "page" in data
        assert "size" in data
        assert data["size"] == 0
        assert "has_next" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_deployments_with_pagination(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        request = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        for _ in range(3):
            rsp = await authenticated_client.post("/api/v1/deployments/", json=request)
            assert rsp.status_code == 201, rsp.json()

        rsp = await authenticated_client.get("/api/v1/deployments/?page=1&limit=10")

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()

        assert "page" in data
        assert "size" in data
        assert data["size"] == 3
        assert "has_next" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_deployments_with_status_filter(self, authenticated_client):
        rsp = await authenticated_client.get(
            f"/api/v1/deployments/?status={StrategyDeploymentStatus.PENDING.value}"
        )

        assert rsp.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_deployments_unauthenticated_returns_401(self, client):
        rsp = await client.get("/api/v1/deployments/")

        assert rsp.status_code == 401


class TestStartDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_start_deployment_returns_200(self, authenticated_client, db_sess):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        deployment = await db_sess.get(StrategyDeployments, deployment_id)
        deployment.status = StrategyDeploymentStatus.STOPPED
        await db_sess.commit()

        rsp = await authenticated_client.post(f"/api/v1/deployments/{deployment_id}/start")

        assert rsp.status_code == 200, rsp.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_start_deployment_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        rsp = await authenticated_client.post(f"/api/v1/deployments/{fake_id}/start")

        assert rsp.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_start_deployment_already_running_returns_400(
        self, authenticated_client, db_sess
    ):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        deployment = await db_sess.get(StrategyDeployments, deployment_id)
        deployment.status = StrategyDeploymentStatus.RUNNING
        await db_sess.commit()

        rsp = await authenticated_client.post(f"/api/v1/deployments/{deployment_id}/start")

        assert rsp.status_code == 400, rsp.json()


class TestStopDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_deployment_returns_200(self, authenticated_client, db_sess):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        deployment = await db_sess.get(StrategyDeployments, deployment_id)
        deployment.status = StrategyDeploymentStatus.RUNNING
        await db_sess.commit()

        rsp = await authenticated_client.post(f"/api/v1/deployments/{deployment_id}/stop")

        assert rsp.status_code == 200, rsp.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_deployment_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        rsp = await authenticated_client.post(f"/api/v1/deployments/{fake_id}/stop")

        assert rsp.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_already_stopped_deployment_returns_200(
        self, authenticated_client, db_sess
    ):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        deployment = await db_sess.get(StrategyDeployments, deployment_id)
        deployment.status = StrategyDeploymentStatus.STOPPED
        await db_sess.commit()

        rsp = await authenticated_client.post(f"/api/v1/deployments/{deployment_id}/stop")

        assert rsp.status_code == 200, rsp.json()


class TestGetDeploymentOrders:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders_returns_200(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        rsp = await authenticated_client.get(f"/api/v1/deployments/{deployment_id}/orders")

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert "page" in data
        assert "size" in data
        assert "has_next" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        rsp = await authenticated_client.get(f"/api/v1/deployments/{fake_id}/orders")

        assert rsp.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders_with_pagination(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        rsp = await authenticated_client.get(
            f"/api/v1/deployments/{deployment_id}/orders?page=1&limit=10"
        )

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert data["page"] == 1


class TestGetDeploymentEvents:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_events_returns_200(self, authenticated_client):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()

        deployment_id = rsp.json()["id"]

        rsp = await authenticated_client.get(f"/api/v1/deployments/{deployment_id}/events")

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert "page" in data
        assert "size" in data
        assert "has_next" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_events_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        rsp = await authenticated_client.get(f"/api/v1/deployments/{fake_id}/events")

        assert rsp.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_events_returns_events_in_data(
        self, authenticated_client, db_sess
    ):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()
        deployment_id = rsp.json()["id"]

        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.RUNNING,
        )
        db_event = DeploymentEvent(
            id=event.id,
            deployment_id=deployment_id,
            event_type=event.type,
            payload=event.model_dump(mode="json"),
            timestamp=event.timestamp,
        )
        db_sess.add(db_event)
        await db_sess.commit()

        rsp = await authenticated_client.get(
            f"/api/v1/deployments/{deployment_id}/events"
        )

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert data["size"] == 1
        assert len(data["data"]) == 1
        assert data["data"][0]["type"] == DeploymentEventType.DEPLOYMENT_STATUS.value
        assert data["data"][0]["deployment_id"] == str(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_events_pagination(self, authenticated_client, db_sess):
        _mock_deployment_runner()

        strategy = await create_strategy(authenticated_client)
        broker_connection_id = await create_broker_connection(authenticated_client)

        payload = {
            "version_id": str(strategy.cur_version_id),
            "broker_connection_id": broker_connection_id,
        }

        rsp = await authenticated_client.post("/api/v1/deployments/", json=payload)
        assert rsp.status_code == 201, rsp.json()
        deployment_id = rsp.json()["id"]

        for i in range(3):
            event = DeploymentStatusChangedEvent(
                deployment_id=deployment_id,
                status=StrategyDeploymentStatus.RUNNING,
            )
            db_event = DeploymentEvent(
                id=event.id,
                deployment_id=deployment_id,
                event_type=event.type,
                payload=event.model_dump(mode="json"),
                timestamp=event.timestamp + i,
            )
            db_sess.add(db_event)
        await db_sess.commit()

        rsp = await authenticated_client.get(
            f"/api/v1/deployments/{deployment_id}/events?page=1&limit=2"
        )

        assert rsp.status_code == 200, rsp.json()
        data = rsp.json()
        assert data["size"] == 2
        assert len(data["data"]) == 2
        assert data["has_next"] is True


class TestDeploymentEndpointsUnauthenticated:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_deployment_unauthenticated_returns_401(self, client):
        payload = {
            "version_id": str(uuid4()),
            "broker_connection_id": str(uuid4()),
        }

        rsp = await client.post("/api/v1/deployments/", json=payload)

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_deployments_unauthenticated_returns_401(self, client):
        rsp = await client.get("/api/v1/deployments/")

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_deployment_unauthenticated_returns_401(self, client):
        rsp = await client.get(f"/api/v1/deployments/{uuid4()}")

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_start_deployment_unauthenticated_returns_401(self, client):
        rsp = await client.post(f"/api/v1/deployments/{uuid4()}/start")

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_deployment_unauthenticated_returns_401(self, client):
        rsp = await client.post(f"/api/v1/deployments/{uuid4()}/stop")

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders_unauthenticated_returns_401(self, client):
        rsp = await client.get(f"/api/v1/deployments/{uuid4()}/orders")

        assert rsp.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_events_unauthenticated_returns_401(self, client):
        rsp = await client.get(f"/api/v1/deployments/{uuid4()}/events")

        assert rsp.status_code == 401
