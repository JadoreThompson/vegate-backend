import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient
from uuid import uuid4

from api.routes.auth.service import AuthService
from api.routes.broker_connections.service import (
    BrokerConnectionsService,
    _BrokerAccount,
)
from api.routes.broker_connections.model import CreateBrokerConnectionRequest
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


class MockBrokerService:
    def __init__(self):
        self._http_sess = AsyncMock()

    async def create_broker_connection(self, request, user_id, db_sess):
        connection_id = uuid4()
        return type('obj', (object,), {
            'id': connection_id,
            'broker': request.broker,
            'account_id': 'mock-account-id',
            'account_number': 'MOCK-001',
        })()

    async def get_broker_connections(self, user_id, db_sess, *, page, limit):
        from api.models import PaginatedResponse
        return PaginatedResponse(
            page=page,
            size=0,
            has_next=False,
            data=[]
        )

    async def get_broker_connection(self, id, user_id, db_sess):
        return None

    async def delete_broker_connection(self, id, user_id, db_sess):
        return True


@pytest.fixture
def mock_broker_service():
    # return MockBrokerService()
    return MagicMock()


@pytest.fixture
def broker_service_fixture(mock_broker_service, monkeypatch):
    monkeypatch.setattr(
        "api.routes.broker_connections.route.broker_connections_service",
        mock_broker_service
    )
    yield mock_broker_service


# @pytest_asyncio.fixture(loop_scope="session")
# async def client():
#     from api.app import app
#
#     async with AsyncClient(
#         transport=ASGITransport(app=app), base_url="http://test"
#     ) as ac:
#         yield ac
#
#
# @pytest_asyncio.fixture(loop_scope="session")
# async def authenticated_client(client):
#     from api.routes.auth.route import auth_service
#
#     auth_service._email_service= AsyncMock
#     code = "TOKEN"
#     auth_service.gen_verification_code = MagicMock(return_value=code)
#     register_payload = {
#         "username": "broker-user",
#         "email": "broker@email.com",
#         "password": "PAssword1@@1",
#     }
#     rsp = await client.post("/auth/register", json=register_payload)
#     assert 200 <= rsp.status_code <= 299
#
#     rsp = await client.post("/auth/verify-email", json={"code": code})
#     assert 200 <= rsp.status_code <= 299
#
#     yield client


class TestCreateBrokerConnection:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_broker_connection_unauthenticated_throws_401(
        self, client
    ):
        payload = {
            "broker": "alpaca",
            "api_key": "test-api-key",
            "secret_key": "test-secret-key",
        }

        res = await client.post("/broker-connections", json=payload)
        assert res.status_code == 401, res.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_broker_connection_returns_200(self, authenticated_client):
        from api.routes.broker_connections.route import broker_connections_service

        broker_connections_service._fetch_alpaca_account_id = AsyncMock(return_value=_BrokerAccount(id="mock-account-id", number="mock-number"))

        payload = {
            "broker": "alpaca",
            "api_key": "test-api-key",
            "secret_key": "test-secret-key",
        }

        res = await authenticated_client.post("/broker-connections", json=payload)

        assert res.status_code == 200, res.json()
        data = res.json()
        assert data["broker"] == "alpaca"
        assert "account_id" in data
        assert "account_number" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_broker_connection_missing_api_key_returns_422(
        self, authenticated_client
    ):
        payload = {
            "broker": "alpaca",
            "secret_key": "test-secret",
        }

        res = await authenticated_client.post("/broker-connections", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_broker_connection_missing_secret_returns_422(
        self, authenticated_client
    ):
        payload = {
            "broker": "alpaca",
            "api_key": "test-key",
        }

        res = await authenticated_client.post("/broker-connections", json=payload)

        assert res.status_code == 422


class TestListBrokerConnections:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_broker_connections_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/broker-connections")

        assert res.status_code == 200
        data = res.json()
        assert "data" in data
        assert "page" in data
        assert "size" in data
        assert "has_next" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_broker_connections_with_pagination(
        self, authenticated_client
    ):
        from api.routes.broker_connections.route import broker_connections_service

        for i in range(15):
            broker_connections_service._fetch_alpaca_account_id = AsyncMock(return_value=_BrokerAccount(id=f"mock-account-id-{i}", number=f"mock-number-{i}"))
            await authenticated_client.post("/broker-connections", json={
                "broker": "alpaca",
                "api_key": "test-api-key",
                "secret_key": "test-secret-key",
            })

        res = await authenticated_client.get("/broker-connections?page=1&limit=10")

        assert res.status_code == 200, res.json()
        data = res.json()
        assert data["page"] == 1, data
        assert data["size"] == 10, data
        assert data["has_next"], data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_broker_connections_with_pagination_second_page(
            self, authenticated_client
    ):
        from api.routes.broker_connections.route import broker_connections_service

        for i in range(15):
            broker_connections_service._fetch_alpaca_account_id = AsyncMock(
                return_value=_BrokerAccount(id=f"mock-account-id-{i}", number=f"mock-number-{i}"))
            await authenticated_client.post("/broker-connections", json={
                "broker": "alpaca",
                "api_key": "test-api-key",
                "secret_key": "test-secret-key",
            })

        res = await authenticated_client.get("/broker-connections?page=2&limit=10")

        assert res.status_code == 200, res.json()
        data = res.json()
        assert data["page"] == 2, data
        assert data["size"] == 5, data
        assert not data["has_next"], data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_broker_connections_with_pagination_first_page(
            self, authenticated_client
    ):
        from api.routes.broker_connections.route import broker_connections_service

        for i in range(10):
            broker_connections_service._fetch_alpaca_account_id = AsyncMock(
                return_value=_BrokerAccount(id=f"mock-account-id-{i}", number=f"mock-number-{i}"))
            await authenticated_client.post("/broker-connections", json={
                "broker": "alpaca",
                "api_key": "test-api-key",
                "secret_key": "test-secret-key",
            })

        res = await authenticated_client.get("/broker-connections?page=1&limit=10")

        assert res.status_code == 200, res.json()
        data = res.json()
        assert data["page"] == 1, data
        assert data["size"] == 10, data
        assert not data["has_next"], data


class TestGetBrokerConnection:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_broker_connection_not_found_returns_404(
        self, authenticated_client
    ):
        fake_id = uuid4()
        res = await authenticated_client.get(f"/broker-connections/{fake_id}")

        assert res.status_code == 404


class TestDeleteBrokerConnection:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_broker_connection_unauthenticated_throws_401(
            self, client
    ):
        res = await client.delete(f"/broker-connections/{uuid4()}")
        assert res.status_code == 401, res.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_broker_connection_returns_204(self, authenticated_client):
        from api.routes.broker_connections.route import broker_connections_service

        broker_connections_service._fetch_alpaca_account_id = AsyncMock(
            return_value=_BrokerAccount(id="mock-account-id", number="mock-number"))

        payload = {
            "broker": "alpaca",
            "api_key": "test-api-key",
            "secret_key": "test-secret-key",
        }

        res = await authenticated_client.post("/broker-connections", json=payload)
        assert res.status_code == 200, res.json()

        data = res.json()
        # connection_id = uuid4()
        connection_id = data['id']

        res = await authenticated_client.delete(
            f"/broker-connections/{connection_id}"
        )

        assert res.status_code == 204

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_broker_connection_not_found_returns_404(
        self, authenticated_client, mock_broker_service
    ):
        mock_broker_service.delete_broker_connection = AsyncMock(return_value=False)

        fake_id = uuid4()
        res = await authenticated_client.delete(f"/broker-connections/{fake_id}")

        assert res.status_code == 404


class TestAlpacaOauthUrl:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_alpaca_oauth_url_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/broker-connections/alpaca/oauth")

        assert res.status_code == 200
        data = res.json()
        assert "url" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_alpaca_oauth_url_unauthenticated_returns_401(
        self, client
    ):
        res = await client.get("/broker-connections/alpaca/oauth")

        assert res.status_code == 401

# TODO: Add mock data to tests
class TestAlpacaOauthCallback:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_oauth_callback_with_code_redirects(self, authenticated_client):
        res = await authenticated_client.get(
            "/broker-connections/alpaca/oauth/callback",
            params={"code": "test-code", "state": "test-state"}
        )

        assert res.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_oauth_callback_with_error_redirects(self, authenticated_client):
        res = await authenticated_client.get(
            "/broker-connections/alpaca/oauth/callback",
            params={"error": "access_denied"}
        )

        assert res.status_code == 200