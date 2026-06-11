import json
import pytest
import pytest_asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from uuid import uuid4, UUID

from sqlalchemy import select

from config import OMS_SESSION_PREFIX, STRATEGY_DEPLOYMENT_EVENTS_KEY
from module.deployment.event.event import (
    DeploymentCancelOrderSubmitted,
    DeploymentEventType,
    DeploymentModifyOrderSubmitted,
    DeploymentOrderAcknowledged,
    DeploymentOrderRejected,
    DeploymentOrderSubmitted,
)
from module.broker_connections.model import BrokerConnections
from module.strategy.model import Strategy
from module.deployment.model import StrategyDeploymentOrders, StrategyDeployments
from module.user.model import User
from core.db import get_db_session
from module.event_bus import EventPublisher
from module.broker.client.alpaca import AlpacaBrokerClient
from module.broker.client.base import BrokerClient
from module.broker.client.exception import BrokerClientException
from vegate.oms.enums import BrokerType, OrderSide, OrderStatus, OrderType
from vegate.oms.schema import Order, OrderRequest
from module.deployment.oms.exception import (
    BrokerConnectionDoesNotExistException,
    DuplicateOrderException,
    InvalidSessionException,
    OrderNotFoundException,
)
from module.deployment.oms.schema import PlaceOrderRequest
from module.deployment.oms.service import OMSService, Session


def make_order(**kwargs) -> Order:
    defaults = {
        "id": str(uuid4()),
        "symbol": "AAPL",
        "quantity": 10.0,
        "filled_quantity": 0.0,
        "notional": None,
        "order_type": OrderType.MARKET,
        "side": OrderSide.BUY,
        "limit_price": None,
        "stop_price": None,
        "avg_fill_price": None,
        "executed_at": None,
        "submitted_at": None,
        "status": OrderStatus.PENDING,
    }
    defaults.update(kwargs)
    return Order(**defaults)


def make_order_request(**kwargs) -> OrderRequest:
    defaults = {
        "symbol": "AAPL",
        "quantity": 10.0,
        "notional": None,
        "order_type": OrderType.MARKET,
        "side": OrderSide.BUY,
        "limit_price": None,
        "stop_price": None,
    }
    defaults.update(kwargs)
    return OrderRequest(**defaults)


def make_place_order_request(**kwargs) -> PlaceOrderRequest:
    defaults = {
        "order": make_order_request(),
        "candle_ts": 1500000000,
    }
    defaults.update(kwargs)
    return PlaceOrderRequest(**defaults)


def make_broker_conn(**kwargs) -> BrokerConnections:
    conn = MagicMock(spec=BrokerConnections)
    conn.broker = BrokerType.ALPACA
    conn.api_key = "test-key"
    conn.secret_key = "test-secret"
    conn.oauth_payload = None
    conn.id = uuid4()
    for k, v in kwargs.items():
        setattr(conn, k, v)
    return conn


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.delete = AsyncMock()
    return redis


@pytest.fixture
def mock_event_publisher():
    pub = AsyncMock(spec=EventPublisher)
    pub.publish = AsyncMock()
    return pub


@pytest.fixture
def oms_service(mock_redis, mock_event_publisher):
    return OMSService(redis_client=mock_redis, event_publisher=mock_event_publisher)


@pytest.fixture
def mock_broker_client():
    client = MagicMock(spec=BrokerClient)
    client.connect = MagicMock()
    client.disconnect = MagicMock()
    client.get_balance = MagicMock(return_value=10000.0)
    client.get_equity = MagicMock(return_value=15000.0)
    client.get_position = MagicMock(return_value=10.0)
    client.place_order = MagicMock(return_value=make_order())
    client.modify_order = MagicMock(return_value=make_order())
    client.cancel_order = MagicMock(return_value=True)
    client.cancel_all_orders = MagicMock(return_value=True)
    client.get_order = MagicMock(return_value=make_order())
    client.get_orders = MagicMock(return_value=[make_order()])
    return client


@pytest.fixture
def mock_session(mock_broker_client):
    deployment_id = uuid4()
    return Session(deployment_id=deployment_id, broker_client=mock_broker_client)


class TestCreateSession:
    """Unit tests for create_session."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_session_success(self, oms_service, mock_broker_client):
        deployment_id = uuid4()
        user_id = uuid4()
        broker_conn = make_broker_conn()

        # Mock DB query result
        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=(user_id, broker_conn))

        mock_db_sess = AsyncMock()
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                oms_service, "_build_broker_client", return_value=mock_broker_client
            ):
                token = await oms_service.create_session(deployment_id)

        assert token is not None
        assert isinstance(token, str)
        assert token in oms_service._broker_clients
        mock_broker_client.connect.assert_called_once()
        oms_service._redis_client.set.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_session_broker_conn_not_found(self, oms_service):
        deployment_id = uuid4()

        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=None)

        mock_db_sess = AsyncMock()
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with pytest.raises(
                BrokerConnectionDoesNotExistException, match=str(deployment_id)
            ):
                await oms_service.create_session(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_session_with_existing_token(
        self, oms_service, mock_broker_client
    ):
        deployment_id = uuid4()
        user_id = uuid4()
        broker_conn = make_broker_conn()
        existing_token = "existing-token-123"

        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=(user_id, broker_conn))

        mock_db_sess = AsyncMock()
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                oms_service, "_build_broker_client", return_value=mock_broker_client
            ):
                token = await oms_service.create_session(
                    deployment_id, existing_token=existing_token
                )

        assert token == existing_token

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_session_duplicate_token_raises(
        self, oms_service, mock_broker_client
    ):
        deployment_id = uuid4()
        user_id = uuid4()
        broker_conn = make_broker_conn()
        existing_token = "duplicate-token"

        # Pre-populate to simulate duplicate
        oms_service._broker_clients[existing_token] = MagicMock()

        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=(user_id, broker_conn))

        mock_db_sess = AsyncMock()
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                oms_service, "_build_broker_client", return_value=mock_broker_client
            ):
                with pytest.raises(ValueError, match="already exists"):
                    await oms_service.create_session(
                        deployment_id, existing_token=existing_token
                    )


class TestCloseSession:
    """Unit tests for close_session."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_close_session_success(self, oms_service, mock_session):
        token = "test-token"
        oms_service._broker_clients[token] = mock_session

        await oms_service.close_session(token)

        mock_session.broker_client.disconnect.assert_called_once()
        oms_service._redis_client.delete.assert_awaited_once_with(
            f"{OMS_SESSION_PREFIX}{token}"
        )
        assert token not in oms_service._broker_clients

    @pytest.mark.asyncio(loop_scope="session")
    async def test_close_session_invalid_token(self, oms_service):
        with pytest.raises(ValueError, match="Invalid or expired token"):
            await oms_service.close_session("invalid-token")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_close_session_disconnect_failure(self, oms_service, mock_session):
        token = "test-token"
        mock_session.broker_client.disconnect = MagicMock(
            side_effect=RuntimeError("disconnect failed")
        )
        oms_service._broker_clients[token] = mock_session

        with pytest.raises(RuntimeError, match="Failed to disconnect"):
            await oms_service.close_session(token)

        # Should still clean up Redis and local cache in finally
        oms_service._redis_client.delete.assert_awaited_once()
        assert token not in oms_service._broker_clients


class TestGetSession:
    """Unit tests for _get_session."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_session_from_memory(self, oms_service, mock_session, mock_redis):
        token = "test-token"
        oms_service._broker_clients[token] = mock_session

        result = await oms_service._get_session(token)

        assert result == mock_session
        assert mock_redis.get.call_count == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_session_from_redis(self, oms_service, mock_broker_client):
        token = "test-token"
        deployment_id = uuid4()
        user_id = uuid4()
        broker_conn = make_broker_conn()

        oms_service._redis_client.get = AsyncMock(
            return_value=json.dumps({"deployment_id": str(deployment_id)}).encode()
        )

        mock_result = MagicMock()
        mock_result.first = MagicMock(return_value=(user_id, broker_conn))

        mock_db_sess = AsyncMock()
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with patch.object(
                oms_service, "_build_broker_client", return_value=mock_broker_client
            ):
                result = await oms_service._get_session(token)

        assert result is not None
        assert result.deployment_id == deployment_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_session_invalid_token(self, oms_service):
        oms_service._redis_client.get = AsyncMock(return_value=None)

        with pytest.raises(InvalidSessionException, match="Invalid session token"):
            await oms_service._get_session("invalid-token")


class TestGetBalance:
    """Unit tests for get_balance."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_balance(self, oms_service, mock_session):
        token = "test-token"
        mock_session.broker_client.get_balance = MagicMock(return_value=1234.0)
        oms_service._broker_clients[token] = mock_session

        result = await oms_service.get_balance(token)

        assert result == 1234.0


class TestGetEquity:
    """Unit tests for get_equity."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_equity(self, oms_service, mock_session):
        token = "test-token"
        mock_session.broker_client.get_equity = MagicMock(return_value=7500.0)
        oms_service._broker_clients[token] = mock_session

        result = await oms_service.get_equity(token)

        assert result == 7500.0


class TestGetPosition:
    """Unit tests for get_position."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_position(self, oms_service, mock_session):
        token = "test-token"
        mock_session.broker_client.get_position = MagicMock(return_value=100.0)
        oms_service._broker_clients[token] = mock_session

        result = await oms_service.get_position(token, "AAPL")

        assert result == 100.0
        mock_session.broker_client.get_position.assert_called_once_with("AAPL")


class TestPlaceOrder:
    """Unit tests for place_order."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_place_order_success(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        order = make_order(id="broker-order-123")
        mock_session.broker_client.place_order = MagicMock(return_value=order)

        request = make_place_order_request()

        # Mock _generate_order_key and _ensure_unique_key
        with patch.object(
            oms_service, "_generate_order_key", AsyncMock(return_value="key-1")
        ):
            
            with patch(
                "module.deployment.oms.service.get_db_session"
            ) as mock_get_db:
                mock_db_sess = AsyncMock()
                mock_db_sess.scalar = AsyncMock(return_value=uuid4())
                mock_db_sess.execute = AsyncMock()
                mock_db_sess.commit = AsyncMock()
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await oms_service.place_order(token, request)

        assert result is not None
        assert result.symbol == "AAPL"
        # Events should be published
        assert mock_event_publisher.publish.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_place_order_broker_exception(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        mock_session.broker_client.place_order = MagicMock(
            side_effect=BrokerClientException("insufficient funds")
        )

        request = make_place_order_request()

        with patch.object(
            oms_service, "_generate_order_key", AsyncMock(return_value="key-1")
        ):
            with patch(
                "module.deployment.oms.service.get_db_session"
            ) as mock_get_db:
                mock_db_sess = AsyncMock()
                mock_db_sess.scalar = AsyncMock(return_value=uuid4())
                mock_db_sess.execute = AsyncMock()
                mock_db_sess.commit = AsyncMock()
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                with pytest.raises(
                    BrokerClientException, match="insufficient funds"
                ):
                    await oms_service.place_order(token, request)

        # Rejection event should be published
        mock_event_publisher.publish.assert_called()
        assert (
            mock_event_publisher.publish.call_args[0][0].type
            == DeploymentEventType.DEPLOYMENT_ORDER_REJECTED
        )


class TestModifyOrder:
    """Unit tests for modify_order."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_modify_order_success(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        modified_order = make_order(id=broker_order_id, limit_price=150.0)
        mock_session.broker_client.modify_order = MagicMock(return_value=modified_order)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute = AsyncMock()
            mock_db_sess.commit = AsyncMock()

            with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await oms_service.modify_order(
                    token, order_id, limit_price=150.0
                )

        assert result.limit_price == 150.0
        assert result.id == order_id
        mock_event_publisher.publish.assert_awaited_once()
        assert (
            mock_event_publisher.publish.call_args[0][0].type
            == DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_modify_order_with_stop_price(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        modified_order = make_order(id=broker_order_id, stop_price=90.0)
        mock_session.broker_client.modify_order = MagicMock(return_value=modified_order)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute = AsyncMock()
            mock_db_sess.commit = AsyncMock()

            with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await oms_service.modify_order(
                    token, order_id, stop_price=90.0
                )

        assert result.stop_price == 90.0


class TestCancelOrder:
    """Unit tests for cancel_order."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_cancel_order_success(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        mock_session.broker_client.cancel_order = MagicMock(return_value=True)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute = AsyncMock()
            mock_db_sess.commit = AsyncMock()

            with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await oms_service.cancel_order(token, order_id)

        assert result is True
        mock_event_publisher.publish.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_cancel_order_failure(
        self, oms_service, mock_session, mock_event_publisher
    ):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        mock_session.broker_client.cancel_order = MagicMock(return_value=False)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute = AsyncMock()
            mock_db_sess.commit = AsyncMock()

            with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
                mock_get_db.return_value.__aenter__ = AsyncMock(
                    return_value=mock_db_sess
                )
                mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

                result = await oms_service.cancel_order(token, order_id)

        assert result is False


class TestGetOrder:
    """Unit tests for get_order."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_order_success(self, oms_service, mock_session):
        token = "test-token"
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        found_order = make_order(id=broker_order_id)
        mock_session.broker_client.get_order = MagicMock(return_value=found_order)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            result = await oms_service.get_order(token, order_id)

        assert result.id == order_id
        assert result.symbol == "AAPL"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_order_not_found(self, oms_service, mock_session):
        token = "test-token"
        oms_service._broker_clients[token] = mock_session

        order_id = uuid4()
        broker_order_id = "broker-123"
        mock_session.broker_client.get_order = MagicMock(return_value=None)

        with patch.object(
            oms_service, "_get_broker_order_id", AsyncMock(return_value=broker_order_id)
        ):
            with pytest.raises(OrderNotFoundException, match=str(order_id)):
                await oms_service.get_order(token, order_id)


class TestCancelAllOrders:
    """Unit tests for cancel_all_orders."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_cancel_all_orders_success(self, oms_service, mock_session):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session
        mock_session.broker_client.cancel_all_orders = MagicMock(return_value=True)

        mock_db_sess = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[uuid4()]))
        )
        mock_db_sess.execute = AsyncMock(return_value=mock_result)
        mock_db_sess.commit = AsyncMock()

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await oms_service.cancel_all_orders(token)

        assert result is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_cancel_all_orders_no_orders(self, oms_service, mock_session):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        mock_db_sess = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars = MagicMock(
            return_value=MagicMock(all=MagicMock(return_value=[]))
        )
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await oms_service.cancel_all_orders(token)

        assert result is True


class TestGetOrders:
    """Unit tests for get_orders."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders(self, oms_service, mock_session):
        token = "test-token"
        deployment_id = uuid4()
        mock_session.deployment_id = deployment_id
        oms_service._broker_clients[token] = mock_session

        broker_order_id = "broker-123"
        internal_id = uuid4()
        order = make_order(id=broker_order_id)
        mock_session.broker_client.get_orders = MagicMock(return_value=[order])

        mock_db_sess = AsyncMock()
        mock_result = MagicMock()
        mock_result.all = MagicMock(return_value=[(internal_id, broker_order_id)])
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await oms_service.get_orders(token, deployment_id)

        assert len(result) == 1
        assert result[0].id == internal_id


class TestBuildBrokerClient:
    """Unit tests for _build_broker_client."""

    def test_build_alpaca_with_api_key(self, oms_service):
        broker_conn = make_broker_conn(
            api_key="my-key",
            secret_key="my-secret",
            oauth_payload=None,
        )
        user_id = uuid4()

        with patch.object(
            AlpacaBrokerClient, "__init__", return_value=None
        ) as mock_init:
            oms_service._build_broker_client(broker_conn, user_id)

        mock_init.assert_called_once_with(
            api_key="my-key",
            secret_key="my-secret",
            oauth_token=None,
        )

    def test_build_alpaca_with_oauth(self, oms_service):
        broker_conn = make_broker_conn(
            api_key=None,
            secret_key=None,
            oauth_payload="encrypted-oauth",
        )
        user_id = uuid4()

        with patch(
            "module.broker_connections.oauth.encryption.EncryptionService.decrypt",
            return_value=json.dumps(
                {
                    "access_token": "oauth-token-123",
                    "token_type": "oauth",
                    "scope": "<scope>",
                    "env": "paper",
                }
            ),
        ):
            with patch.object(
                AlpacaBrokerClient, "__init__", return_value=None
            ) as mock_init:
                oms_service._build_broker_client(broker_conn, user_id)

        mock_init.assert_called_once_with(
            api_key=None,
            secret_key=None,
            oauth_token="oauth-token-123",
        )

    def test_build_unsupported_broker_raises(self, oms_service):
        broker_conn = make_broker_conn()
        broker_conn.broker = "unsupported"
        user_id = uuid4()

        with pytest.raises(ValueError, match="Unsupported broker"):
            oms_service._build_broker_client(broker_conn, user_id)


class TestGetBrokerOrderId:
    """Unit tests for _get_broker_order_id."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_broker_order_id_success(self, oms_service):
        order_id = uuid4()
        broker_order_id = "broker-123"

        mock_db_sess = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar = MagicMock(return_value=broker_order_id)
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await oms_service._get_broker_order_id(order_id)

        assert result == broker_order_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_broker_order_id_not_found(self, oms_service):
        order_id = uuid4()

        mock_db_sess = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar = MagicMock(return_value=None)
        mock_db_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.deployment.oms.service.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_db_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            with pytest.raises(OrderNotFoundException, match=str(order_id)):
                await oms_service._get_broker_order_id(order_id)
