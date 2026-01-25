import json
import pytest
from unittest.mock import MagicMock
from uuid import uuid4

from config import REDIS_ORDER_EVENTS_KEY, REDIS_SNAPSHOT_EVENTS_KEY
from enums import OrderSide, OrderStatus, OrderType, Timeframe, SnapshotType
from events.order import OrderEventType
from events.snapshot import SnapshotEventType
from lib.brokers.proxy import ProxyBroker
from lib.brokers.backtest import BacktestBroker
from models import OrderRequest, OHLC


# Fixtures
@pytest.fixture
def deployment_id():
    """Generate a deployment ID for testing."""
    return uuid4()


@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client."""
    return MagicMock()


@pytest.fixture
def underlying_broker():
    """Create an underlying backtest broker."""
    return BacktestBroker(starting_balance=100000.0)


@pytest.fixture
def proxy_broker(deployment_id, underlying_broker, mock_redis_client):
    """Create a proxy broker instance."""
    return ProxyBroker(
        deployment_id=deployment_id,
        broker=underlying_broker,
        redis_client=mock_redis_client,
    )


@pytest.fixture
def sample_candle():
    """Create a sample OHLC candle for testing."""
    return OHLC(
        symbol="AAPL",
        timestamp=1704103800,  # 2024-01-01 09:30:00 UTC
        open=100.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )


@pytest.fixture
def mock_candle_stream():
    """Create a list of mock candles for streaming."""
    candles = []
    for i in range(3):
        candle = OHLC(
            symbol="AAPL",
            timestamp=1704103800 + (i * 60),
            open=100.0 + i,
            high=102.0 + i,
            low=98.0 + i,
            close=101.0 + i,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        candles.append(candle)
    return candles


# Initialization Tests
def test_proxy_broker_initialization(proxy_broker, deployment_id, underlying_broker):
    """Test proxy broker initializes correctly."""
    assert proxy_broker.deployment_id == deployment_id
    assert proxy_broker.broker == underlying_broker
    assert proxy_broker.supports_async == underlying_broker.supports_async


# Proxy Method Tests
def test_get_balance_proxies_to_underlying_broker(proxy_broker, underlying_broker):
    """Test get_balance proxies to underlying broker."""
    balance = proxy_broker.get_balance()
    assert balance == underlying_broker.get_balance()
    assert balance == 100000.0


def test_get_equity_proxies_to_underlying_broker(proxy_broker, underlying_broker):
    """Test get_equity proxies to underlying broker."""
    equity = proxy_broker.get_equity()
    assert equity == underlying_broker.get_equity()
    assert equity == 100000.0


# Order Event Emission Tests
def test_place_order_emits_order_placed_event(
    proxy_broker, deployment_id, mock_redis_client, sample_candle
):
    """Test place_order emits OrderPlaced event."""
    proxy_broker.broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )

    order = proxy_broker.place_order(order_request)

    # Verify order was placed
    assert order.status == OrderStatus.FILLED
    assert order.quantity == 10.0

    # Verify event was published
    assert mock_redis_client.publish.called
    call_args = mock_redis_client.publish.call_args
    channel = call_args[0][0]
    event_json = call_args[0][1]

    assert channel == REDIS_ORDER_EVENTS_KEY

    # Verify event data
    event_data = json.loads(event_json)
    assert event_data["type"] == OrderEventType.ORDER_PLACED
    assert event_data["deployment_id"] == str(deployment_id)
    assert event_data["order"]["symbol"] == "AAPL"
    assert event_data["order"]["quantity"] == 10.0


def test_cancel_order_emits_order_cancelled_event(
    proxy_broker, deployment_id, mock_redis_client, sample_candle
):
    """Test cancel_order emits OrderCancelled event."""
    proxy_broker.broker._cur_candle = sample_candle

    # Place a limit order first
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    order = proxy_broker.place_order(order_request)

    # Reset mock to clear place_order call
    mock_redis_client.reset_mock()

    # Cancel the order
    success = proxy_broker.cancel_order(order.order_id)

    # Verify cancellation succeeded
    assert success is True

    # Verify event was published
    assert mock_redis_client.publish.called
    call_args = mock_redis_client.publish.call_args
    channel = call_args[0][0]
    event_json = call_args[0][1]

    # Verify channel
    assert channel == REDIS_ORDER_EVENTS_KEY

    # Verify event data
    event_data = json.loads(event_json)
    assert event_data["type"] == OrderEventType.ORDER_CANCELLED
    assert event_data["deployment_id"] == str(deployment_id)
    assert event_data["order_id"] == order.order_id
    assert event_data["success"] is True


def test_modify_order_emits_order_modified_event(
    proxy_broker, deployment_id, mock_redis_client, sample_candle
):
    """Test modify_order emits OrderModified event."""
    proxy_broker.broker._cur_candle = sample_candle

    # Place a limit order first
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    order = proxy_broker.place_order(order_request)

    # Reset mock to clear place_order call
    mock_redis_client.reset_mock()

    # Modify the order
    modified_order = proxy_broker.modify_order(order.order_id, limit_price=98.0)

    # Verify modification succeeded
    assert modified_order.limit_price == 98.0

    # Verify event was published
    assert mock_redis_client.publish.called
    call_args = mock_redis_client.publish.call_args
    channel = call_args[0][0]
    event_json = call_args[0][1]

    assert channel == REDIS_ORDER_EVENTS_KEY

    # Verify event data
    event_data = json.loads(event_json)
    assert event_data["type"] == OrderEventType.ORDER_MODIFIED
    assert event_data["deployment_id"] == str(deployment_id)
    assert event_data["order"]["order_id"] == order.order_id
    assert event_data["order"]["limit_price"] == 98.0
    assert event_data["success"] is True


# Snapshot Event Emission Tests
def test_stream_candles_emits_snapshot_events(
    proxy_broker, deployment_id, mock_redis_client, mock_candle_stream
):
    """Test stream_candles emits snapshot events for each candle."""
    # Mock the underlying broker's stream_candles to return our mock candles
    proxy_broker.broker.stream_candles = MagicMock(
        return_value=iter(mock_candle_stream)
    )

    # Stream candles
    candles_received = list(proxy_broker.stream_candles("AAPL", Timeframe.m1))

    # Verify all candles were yielded
    assert len(candles_received) == 3

    # Verify Redis publish was called
    # Should be called 6 times (2 snapshots per candle: equity + balance)
    assert mock_redis_client.publish.call_count == 6

    # Get all publish calls
    publish_calls = mock_redis_client.publish.call_args_list

    # Verify each pair of calls (equity and balance)
    for i in range(3):
        equity_call = publish_calls[i * 2]
        balance_call = publish_calls[i * 2 + 1]

        # Verify equity snapshot
        assert equity_call[0][0] == REDIS_SNAPSHOT_EVENTS_KEY
        equity_event = json.loads(equity_call[0][1])
        assert equity_event["type"] == SnapshotEventType.SNAPSHOT_CREATED
        assert equity_event["deployment_id"] == str(deployment_id)
        assert equity_event["snapshot_type"] == SnapshotType.EQUITY
        assert "value" in equity_event

        # Verify balance snapshot
        assert balance_call[0][0] == REDIS_SNAPSHOT_EVENTS_KEY
        balance_event = json.loads(balance_call[0][1])
        assert balance_event["type"] == SnapshotEventType.SNAPSHOT_CREATED
        assert balance_event["deployment_id"] == str(deployment_id)
        assert balance_event["snapshot_type"] == SnapshotType.BALANCE
        assert "value" in balance_event


def test_stream_candles_snapshot_values_are_correct(
    proxy_broker, deployment_id, mock_redis_client, sample_candle
):
    """Test snapshot events contain correct equity and balance values."""
    # Set up broker with a position
    proxy_broker.broker._cur_candle = sample_candle

    # Place an order to create a position
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    proxy_broker.broker.place_order(order_request)

    # Reset mock to clear place_order calls
    mock_redis_client.reset_mock()

    # Create a single candle stream
    single_candle = [sample_candle]
    proxy_broker.broker.stream_candles = MagicMock(return_value=iter(single_candle))

    # Stream candles
    list(proxy_broker.stream_candles("AAPL", Timeframe.m1))

    # Get the published events
    publish_calls = mock_redis_client.publish.call_args_list

    # Parse equity and balance events
    equity_event = json.loads(publish_calls[0][0][1])
    balance_event = json.loads(publish_calls[1][0][1])

    # Verify values
    expected_balance = 100000.0 - (10.0 * 101.0)  # Starting balance - purchase cost
    expected_equity = expected_balance + (10.0 * 101.0)  # Balance + holdings value

    assert equity_event["value"] == expected_equity
    assert balance_event["value"] == expected_balance


@pytest.mark.asyncio
async def test_stream_candles_async_emits_snapshot_events(
    proxy_broker, deployment_id, mock_redis_client, mock_candle_stream
):
    """Test stream_candles_async emits snapshot events for each candle."""

    # Create an async generator for mock candles
    async def mock_async_stream():
        for candle in mock_candle_stream:
            yield candle

    # Mock the underlying broker's stream_candles_async
    proxy_broker.broker.stream_candles_async = MagicMock(
        return_value=mock_async_stream()
    )

    # Stream candles asynchronously
    candles_received = []
    async for candle in proxy_broker.stream_candles_async("AAPL", Timeframe.m1):
        candles_received.append(candle)

    # Verify all candles were yielded
    assert len(candles_received) == 3

    # Verify Redis publish was called
    # Should be called 6 times (2 snapshots per candle: equity + balance)
    assert mock_redis_client.publish.call_count == 6

    # Get all publish calls
    publish_calls = mock_redis_client.publish.call_args_list

    from config import REDIS_SNAPSHOT_EVENTS_KEY

    # Verify each pair of calls (equity and balance)
    for i in range(3):
        equity_call = publish_calls[i * 2]
        balance_call = publish_calls[i * 2 + 1]

        # Verify equity snapshot
        assert equity_call[0][0] == REDIS_SNAPSHOT_EVENTS_KEY
        equity_event = json.loads(equity_call[0][1])
        assert equity_event["type"] == SnapshotEventType.SNAPSHOT_CREATED
        assert equity_event["deployment_id"] == str(deployment_id)
        assert equity_event["snapshot_type"] == SnapshotType.EQUITY

        # Verify balance snapshot
        assert balance_call[0][0] == REDIS_SNAPSHOT_EVENTS_KEY
        balance_event = json.loads(balance_call[0][1])
        assert balance_event["type"] == SnapshotEventType.SNAPSHOT_CREATED
        assert balance_event["deployment_id"] == str(deployment_id)
        assert balance_event["snapshot_type"] == SnapshotType.BALANCE


def test_stream_candles_with_multiple_trades(
    proxy_broker, deployment_id, mock_redis_client, mock_candle_stream
):
    """Test snapshot events reflect changing equity/balance with multiple trades."""
    # Set initial candle
    proxy_broker.broker._cur_candle = mock_candle_stream[0]

    # Place initial buy order
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    proxy_broker.broker.place_order(order_request)

    # Reset mock
    mock_redis_client.reset_mock()

    # Mock stream to return candles
    proxy_broker.broker.stream_candles = MagicMock(
        return_value=iter(mock_candle_stream)
    )

    # Stream candles
    list(proxy_broker.stream_candles("AAPL", Timeframe.m1))

    # Verify snapshots were emitted
    assert mock_redis_client.publish.call_count == 6

    # Parse all equity snapshots
    publish_calls = mock_redis_client.publish.call_args_list
    equity_values = []

    for i in range(3):
        equity_event = json.loads(publish_calls[i * 2][0][1])
        equity_values.append(equity_event["value"])

    # Verify equity values are tracked across candles
    # All should be valid floats
    for value in equity_values:
        assert isinstance(value, (int, float))
        assert value > 0


def test_stream_candles_empty_stream(proxy_broker, mock_redis_client):
    """Test stream_candles with empty stream doesn't emit events."""
    # Mock empty stream
    proxy_broker.broker.stream_candles = MagicMock(return_value=iter([]))

    # Stream candles
    candles_received = list(proxy_broker.stream_candles("AAPL", Timeframe.m1))

    # Verify no candles received
    assert len(candles_received) == 0

    # Verify no events published
    assert mock_redis_client.publish.call_count == 0


def test_get_order_proxies_correctly(proxy_broker, sample_candle):
    """Test get_order proxies to underlying broker."""
    proxy_broker.broker._cur_candle = sample_candle

    # Place an order
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    placed_order = proxy_broker.place_order(order_request)

    # Get the order through proxy
    retrieved_order = proxy_broker.get_order(placed_order.order_id)

    # Verify order was retrieved
    assert retrieved_order is not None
    assert retrieved_order.order_id == placed_order.order_id


def test_get_orders_proxies_correctly(proxy_broker, sample_candle):
    """Test get_orders proxies to underlying broker."""
    proxy_broker.broker._cur_candle = sample_candle

    # Place multiple orders
    for i in range(3):
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0 + i,
        )
        proxy_broker.place_order(order_request)

    # Get all orders through proxy
    orders = proxy_broker.get_orders()

    # Verify orders were retrieved
    assert len(orders) == 3


def test_cancel_all_orders_proxies_correctly(proxy_broker, sample_candle):
    """Test cancel_all_orders proxies to underlying broker."""
    proxy_broker.broker._cur_candle = sample_candle

    # Place multiple limit orders
    for i in range(3):
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=10.0,
            limit_price=99.0 - i,
        )
        proxy_broker.place_order(order_request)

    # Cancel all orders
    result = proxy_broker.cancel_all_orders()

    # Verify cancellation succeeded
    assert result is True

    # Verify all orders are cancelled
    orders = proxy_broker.get_orders()
    for order in orders:
        assert order.status == OrderStatus.CANCELLED
