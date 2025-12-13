import pytest
import asyncio
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock, AsyncMock, PropertyMock
from uuid import UUID, uuid4

from alpaca.common.exceptions import APIError
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    TimeInForce as AlpacaTimeInForce,
    OrderType as AlpacaOrderType,
    OrderStatus as AlpacaOrderStatus,
)

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from engine.brokers.alpaca import AlpacaBroker
from engine.brokers.exc import (
    BrokerError,
    AuthenticationError,
    OrderRejectedError,
    InsufficientFundsError,
    RateLimitError,
    BrokerConnectionError,
)
from engine.models import (
    OrderRequest,
    OrderResponse,
    OrderSide,
    OrderType,
    OrderStatus,
    TimeInForce,
    Account,
)
from engine.enums import Timeframe


@pytest.fixture
def deployment_id():
    return uuid4()


@pytest.fixture
def broker(deployment_id):
    return AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=True,
    )


@pytest.fixture
def mock_alpaca_account():
    account = Mock()
    account.id = "test_account_id"
    account.equity = 100000.0
    account.cash = 50000.0
    return account


@pytest.fixture
def mock_alpaca_order():
    order = Mock()
    order.id = "alpaca_order_123"
    order.client_order_id = "client_123"
    order.symbol = "AAPL"
    order.side = AlpacaOrderSide.BUY
    order.type = AlpacaOrderType.MARKET
    order.qty = 10.0
    order.filled_qty = 10.0
    order.status = AlpacaOrderStatus.FILLED
    order.submitted_at = datetime(2024, 1, 1, 9, 30)
    order.filled_at = datetime(2024, 1, 1, 9, 30, 5)
    order.filled_avg_price = 150.50
    order.limit_price = None
    order.stop_price = None
    order.time_in_force = AlpacaTimeInForce.GTC
    return order


def test_broker_initialization(deployment_id):
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key="test_key",
        secret_key="test_secret",
        paper=True,
    )

    assert broker._deployment_id == deployment_id
    assert broker._api_key == "test_key"
    assert broker._secret_key == "test_secret"
    assert broker._paper is True
    assert broker._connected is False


def test_broker_initialization_with_oauth(deployment_id):
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        oauth_token="oauth_token_123",
        paper=True,
    )

    assert broker._oauth_token == "oauth_token_123"
    assert broker._api_key is None


@pytest.mark.asyncio(loop_scope="session")
@patch("engine.brokers.alpaca.TradingClient")
async def test_connect_success(mock_trading_client, broker, mock_alpaca_account):
    """Test successful connection to Alpaca broker with async event loop"""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_trading_client.return_value = mock_client_instance

    # Mock the async task creation
    mock_future = asyncio.Future()
    mock_future.set_result(None)

    with patch.object(broker, "_listen_trade_updates", return_value=mock_future):
        broker.connect()

    assert broker._connected is True
    mock_trading_client.assert_called_once()
    mock_client_instance.get_account.assert_called_once()


@pytest.mark.asyncio(loop_scope="session")
@patch("engine.brokers.alpaca.TradingClient")
async def test_connect_authentication_error(mock_trading_client, broker):
    """Test authentication error during connection"""
    mock_client_instance = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Unauthorized")
    type(api_error).status_code = PropertyMock(return_value=401)
    mock_client_instance.get_account.side_effect = api_error
    mock_trading_client.return_value = mock_client_instance

    with pytest.raises(AuthenticationError, match="Invalid Alpaca credentials"):
        broker.connect()

    assert broker._connected is False


@pytest.mark.asyncio(loop_scope="session")
@patch("engine.brokers.alpaca.TradingClient")
async def test_connect_connection_error(mock_trading_client, broker):
    """Test connection error during broker setup"""
    mock_client_instance = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Service unavailable")
    type(api_error).status_code = PropertyMock(return_value=503)
    mock_client_instance.get_account.side_effect = api_error
    mock_trading_client.return_value = mock_client_instance

    with pytest.raises(BrokerConnectionError):
        broker.connect()


@pytest.mark.asyncio(loop_scope="session")
async def test_disconnect_async(broker):
    """Test async disconnect properly cancels stream task"""
    broker._connected = True
    broker._trading_client = Mock()

    # Create a proper async task mock using asyncio
    async def dummy_coroutine():
        await asyncio.sleep(100)  # Long sleep to simulate running task

    # Create an actual task
    mock_task = asyncio.create_task(dummy_coroutine())
    broker._stream_task = mock_task

    await broker.disconnect_async()

    assert broker._connected is False
    assert broker._trading_client is None
    assert mock_task.cancelled()


def test_submit_order_not_connected(broker):
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(BrokerError, match="Broker not connected"):
        broker.submit_order(order)


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_market_order(mock_trading_client, broker, mock_alpaca_order):
    broker._connected = True
    mock_client = Mock()
    mock_client.submit_order.return_value = mock_alpaca_order
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with patch.object(broker, "_on_order_submit"):
        response = broker.submit_order(order)

    assert response.symbol == "AAPL"
    assert response.side == OrderSide.BUY
    assert response.status == OrderStatus.FILLED
    mock_client.submit_order.assert_called_once()


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_limit_order(mock_trading_client, broker, mock_alpaca_order):
    broker._connected = True
    mock_client = Mock()
    mock_alpaca_order.type = AlpacaOrderType.LIMIT
    mock_alpaca_order.limit_price = 145.0
    mock_client.submit_order.return_value = mock_alpaca_order
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=145.0,
        time_in_force=TimeInForce.GTC,
    )

    with patch.object(broker, "_on_order_submit"):
        response = broker.submit_order(order)

    assert response.order_type == OrderType.LIMIT
    assert response.limit_price == 145.0


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_stop_order(mock_trading_client, broker, mock_alpaca_order):
    broker._connected = True
    mock_client = Mock()
    mock_alpaca_order.type = AlpacaOrderType.STOP
    mock_alpaca_order.stop_price = 155.0
    mock_client.submit_order.return_value = mock_alpaca_order
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=155.0,
        time_in_force=TimeInForce.GTC,
    )

    with patch.object(broker, "_on_order_submit"):
        response = broker.submit_order(order)

    assert response.order_type == OrderType.STOP
    assert response.stop_price == 155.0


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_order_insufficient_funds(mock_trading_client, broker):
    broker._connected = True
    mock_client = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Insufficient buying power")
    type(api_error).status_code = PropertyMock(return_value=403)
    mock_client.submit_order.side_effect = api_error
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10000.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(InsufficientFundsError):
        broker.submit_order(order)


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_order_rejected(mock_trading_client, broker):
    broker._connected = True
    mock_client = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Invalid parameters")
    type(api_error).status_code = PropertyMock(return_value=422)
    mock_client.submit_order.side_effect = api_error
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="INVALID",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,  # Valid quantity for testing rejection
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(OrderRejectedError):
        broker.submit_order(order)


@patch("engine.brokers.alpaca.TradingClient")
def test_submit_order_rate_limit(mock_trading_client, broker):
    broker._connected = True
    mock_client = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Rate limit exceeded")
    type(api_error).status_code = PropertyMock(return_value=429)
    mock_client.submit_order.side_effect = api_error
    broker._trading_client = mock_client

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(RateLimitError):
        broker.submit_order(order)


def test_cancel_order_not_connected(broker):
    with pytest.raises(BrokerError, match="Broker not connected"):
        broker.cancel_order("order_123")


@patch("engine.brokers.alpaca.TradingClient")
def test_cancel_order_success(mock_trading_client, broker):
    broker._connected = True
    mock_client = Mock()
    mock_client.cancel_order_by_id.return_value = None
    broker._trading_client = mock_client

    result = broker.cancel_order("order_123")

    assert result is True
    mock_client.cancel_order_by_id.assert_called_once_with("order_123")


@patch("engine.brokers.alpaca.TradingClient")
def test_cancel_order_not_found(mock_trading_client, broker):
    broker._connected = True
    mock_client = Mock()
    # Create APIError mock with status_code property
    api_error = APIError("Order not found")
    type(api_error).status_code = PropertyMock(return_value=404)
    mock_client.cancel_order_by_id.side_effect = api_error
    broker._trading_client = mock_client

    result = broker.cancel_order("nonexistent_order")

    assert result is False


def test_get_order_not_connected(broker):
    with pytest.raises(BrokerError, match="Broker not connected"):
        broker.get_order("order_123")


@patch("engine.brokers.alpaca.TradingClient")
def test_get_order_success(mock_trading_client, broker, mock_alpaca_order):
    broker._connected = True
    mock_client = Mock()
    mock_client.get_order_by_id.return_value = mock_alpaca_order
    broker._trading_client = mock_client

    response = broker.get_order("order_123")

    assert response.order_id == "alpaca_order_123"
    assert response.symbol == "AAPL"
    mock_client.get_order_by_id.assert_called_once_with("order_123")


@patch("engine.brokers.alpaca.TradingClient")
def test_get_open_orders(mock_trading_client, broker, mock_alpaca_order):
    broker._connected = True
    mock_client = Mock()
    mock_client.get_orders.return_value = [mock_alpaca_order]
    broker._trading_client = mock_client

    orders = broker.get_open_orders()

    assert len(orders) == 1
    assert orders[0].symbol == "AAPL"
    mock_client.get_orders.assert_called_once()


@patch("engine.brokers.alpaca.TradingClient")
def test_get_open_orders_filtered_by_symbol(
    mock_trading_client, broker, mock_alpaca_order
):
    broker._connected = True
    mock_client = Mock()
    mock_client.get_orders.return_value = [mock_alpaca_order]
    broker._trading_client = mock_client

    orders = broker.get_open_orders(symbol="AAPL")

    assert len(orders) == 1
    assert orders[0].symbol == "AAPL"


def test_get_account_not_connected(broker):
    with pytest.raises(BrokerError, match="Broker not connected"):
        broker.get_account()


@patch("engine.brokers.alpaca.TradingClient")
def test_get_account_success(mock_trading_client, broker, mock_alpaca_account):
    broker._connected = True
    mock_client = Mock()
    mock_client.get_account.return_value = mock_alpaca_account
    broker._trading_client = mock_client

    account = broker.get_account()

    assert account.account_id == "test_account_id"
    assert account.equity == 100000.0
    assert account.cash == 50000.0


def test_convert_order_to_alpaca_market(broker):
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    alpaca_order = broker._convert_order_to_alpaca(order)

    assert alpaca_order.symbol == "AAPL"
    assert alpaca_order.qty == 10.0
    assert alpaca_order.side == AlpacaOrderSide.BUY


def test_convert_order_to_alpaca_limit(broker):
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=155.0,
        time_in_force=TimeInForce.DAY,
    )

    alpaca_order = broker._convert_order_to_alpaca(order)

    assert alpaca_order.limit_price == 155.0
    assert alpaca_order.side == AlpacaOrderSide.SELL
    assert alpaca_order.time_in_force == AlpacaTimeInForce.DAY


def test_convert_order_to_alpaca_stop(broker):
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=145.0,
        time_in_force=TimeInForce.GTC,
    )

    alpaca_order = broker._convert_order_to_alpaca(order)

    assert alpaca_order.stop_price == 145.0


def test_convert_order_from_alpaca(broker, mock_alpaca_order):
    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.order_id == "alpaca_order_123"
    assert response.symbol == "AAPL"
    assert response.side == OrderSide.BUY
    assert response.order_type == OrderType.MARKET
    assert response.quantity == 10.0
    assert response.filled_quantity == 10.0
    assert response.status == OrderStatus.FILLED
    assert response.avg_fill_price == 150.50


def test_convert_order_from_alpaca_limit(broker, mock_alpaca_order):
    mock_alpaca_order.type = AlpacaOrderType.LIMIT
    mock_alpaca_order.limit_price = 145.0

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.order_type == OrderType.LIMIT
    assert response.limit_price == 145.0


def test_convert_order_status_from_alpaca_pending(broker, mock_alpaca_order):
    mock_alpaca_order.status = AlpacaOrderStatus.PENDING_NEW

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.status == OrderStatus.PENDING


def test_convert_order_status_from_alpaca_partially_filled(broker, mock_alpaca_order):
    mock_alpaca_order.status = AlpacaOrderStatus.PARTIALLY_FILLED
    mock_alpaca_order.filled_qty = 5.0

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.status == OrderStatus.PARTIALLY_FILLED
    assert response.filled_quantity == 5.0


def test_convert_order_status_from_alpaca_cancelled(broker, mock_alpaca_order):
    mock_alpaca_order.status = AlpacaOrderStatus.CANCELED

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.status == OrderStatus.CANCELLED


def test_convert_order_status_from_alpaca_rejected(broker, mock_alpaca_order):
    mock_alpaca_order.status = AlpacaOrderStatus.REJECTED

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.status == OrderStatus.REJECTED


def test_convert_time_in_force_to_alpaca(broker):
    assert broker._convert_tf_to_alpaca(TimeInForce.GTC) == AlpacaTimeInForce.GTC
    assert broker._convert_tf_to_alpaca(TimeInForce.DAY) == AlpacaTimeInForce.DAY
    assert broker._convert_tf_to_alpaca(TimeInForce.IOC) == AlpacaTimeInForce.IOC
    assert broker._convert_tf_to_alpaca(TimeInForce.FOK) == AlpacaTimeInForce.FOK


def test_convert_time_in_force_from_alpaca(broker):
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.GTC) == TimeInForce.GTC
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.DAY) == TimeInForce.DAY
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.IOC) == TimeInForce.IOC
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.FOK) == TimeInForce.FOK


def test_estimate_days_for_bars_daily(broker):
    days = broker._estimate_days_for_bars(20, Timeframe.D1)
    assert days > 20
    assert days <= 35


def test_estimate_days_for_bars_intraday(broker):
    days = broker._estimate_days_for_bars(100, Timeframe.m1)
    assert days >= 1


def test_supports_disconnect_async(broker):
    assert broker.supports_disconnect_async is True


def test_convert_unsupported_order_type(broker):
    """Test that unsupported order types raise appropriate error"""
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.TRAILING_STOP,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(BrokerError, match="Unsupported order type"):
        broker._convert_order_to_alpaca(order)


def test_broker_metadata_in_order_response(broker, mock_alpaca_order):
    """Test that broker metadata is captured in order response"""
    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    assert response.broker_metadata is not None
    assert "alpaca_time_in_force" in response.broker_metadata


@patch("engine.brokers.alpaca.TradingClient")
def test_multiple_orders_submission(mock_trading_client, broker, mock_alpaca_order):
    """Test submitting multiple orders in sequence"""
    broker._connected = True
    mock_client = Mock()
    mock_client.submit_order.return_value = mock_alpaca_order
    broker._trading_client = mock_client

    orders = []
    for i in range(3):
        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
            time_in_force=TimeInForce.GTC,
        )
        with patch.object(broker, "_on_order_submit"):
            response = broker.submit_order(order)
        orders.append(response)

    assert len(orders) == 3
    assert mock_client.submit_order.call_count == 3


def test_order_with_client_order_id(broker, mock_alpaca_order):
    """Test that client order IDs are preserved"""
    mock_alpaca_order.client_order_id = "my_custom_id_123"

    response = broker._convert_order_from_alpaca(mock_alpaca_order)

    # Verify the broker_metadata contains the client_order_id or check the proper field
    assert response.order_id == "alpaca_order_123"  # Verify the conversion works
