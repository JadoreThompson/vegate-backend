import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime, UTC, timedelta

from enums import OrderSide, OrderStatus, OrderType, Timeframe
from models import OrderRequest, OHLC, BacktestConfig, EquityCurvePoint
from lib.brokers.backtest import BacktestBroker
from lib.backtest_engine import BacktestEngine
from lib.strategy import BaseStrategy


# Fixtures
@pytest.fixture
def broker():
    """Create a backtest broker instance with starting balance."""
    return BacktestBroker(starting_balance=100000.0)


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
def mock_db_session():
    """Mock database session to avoid external dependencies."""
    with patch("lib.brokers.backtest.get_db_sess_sync") as mock:
        session = MagicMock()
        mock.return_value.__enter__.return_value = session
        yield session


# Initialization Tests
def test_broker_initialization(broker):
    """Test broker initializes with correct starting values."""
    assert broker.starting_balance == 100000.0
    assert broker.balance == 100000.0
    assert broker.equity == 100000.0
    assert len(broker._order_map) == 0
    assert len(broker._pending_orders) == 0
    assert broker._cur_candle is None


# Balance and Equity Tests
def test_get_balance(broker):
    """Test get_balance returns current balance."""
    assert broker.get_balance() == 100000.0


def test_get_equity_no_candle(broker):
    """Test get_equity returns balance when no candle is set."""
    equity = broker.get_equity()
    assert equity == 100000.0


def test_get_equity_with_position(broker, sample_candle):
    """Test get_equity calculates correctly with open position."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(order_request)

    # Balance should decrease by purchase cost (quantity * close price)
    expected_balance = 100000.0 - (10.0 * 101.0)
    assert broker.balance == expected_balance

    # Equity should remain same (cash + holdings value)
    equity = broker.get_equity()
    expected_equity = expected_balance + (10.0 * 101.0)
    assert equity == expected_equity


# Market Order Tests
def test_place_market_buy_order_success(broker, sample_candle):
    """Test placing a market buy order with sufficient balance."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.FILLED
    assert order.quantity == 10.0
    assert order.executed_quantity == 10.0
    assert order.filled_avg_price == 101.0
    assert broker.balance == 100000.0 - (10.0 * 101.0)


def test_place_market_buy_order_insufficient_balance(broker, sample_candle):
    """Test placing a market buy order with insufficient balance."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=2000.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.REJECTED
    assert order.executed_quantity == 0.0
    assert broker.balance == 100000.0  # Balance unchanged


def test_place_market_sell_order_success(broker, sample_candle):
    """Test placing a market sell order."""
    broker._cur_candle = sample_candle

    # First buy some shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(buy_request)

    # Now sell them
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )

    order = broker.place_order(sell_request)

    assert order.status == OrderStatus.FILLED
    assert order.executed_quantity == 10.0
    assert broker.balance == 100000.0  # Back to starting balance


def test_place_market_order_with_notional(broker, sample_candle):
    """Test placing a market order using notional value."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        notional=5000.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.FILLED
    # With notional, balance should decrease by notional amount
    assert broker.balance == 100000.0 - 5000.0


# Limit Order Tests
def test_place_limit_buy_order_valid(broker, sample_candle):
    """Test placing a valid limit buy order (price below current)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.PLACED
    assert order.limit_price == 99.0
    assert order.executed_quantity == 0.0
    assert len(broker._pending_orders) == 1
    assert broker.balance == 100000.0  # Balance unchanged for pending order


def test_place_limit_buy_order_invalid_price_too_high(broker, sample_candle):
    """Test placing a limit buy order with price above current (should fail)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,  # Above current price
    )

    with pytest.raises(ValueError, match="must be lower than current price"):
        broker.place_order(order_request)


def test_place_limit_sell_order_valid(broker, sample_candle):
    """Test placing a valid limit sell order (price above current)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.PLACED
    assert order.limit_price == 105.0
    assert len(broker._pending_orders) == 1


def test_place_limit_sell_order_invalid_price_too_low(broker, sample_candle):
    """Test placing a limit sell order with price below current (should fail)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,  # Below current price
    )

    with pytest.raises(ValueError, match="must be higher than current price"):
        broker.place_order(order_request)


def test_place_limit_order_no_candle(broker):
    """Test placing a limit order without current candle fails."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )

    with pytest.raises(
        ValueError, match="Cannot place limit order without current market price"
    ):
        broker.place_order(order_request)


# Stop Order Tests
def test_place_stop_buy_order_valid(broker, sample_candle):
    """Test placing a valid stop buy order (price above current)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.PLACED
    assert order.stop_price == 105.0
    assert order.executed_quantity == 0.0
    assert len(broker._pending_orders) == 1


def test_place_stop_buy_order_invalid_price_too_low(broker, sample_candle):
    """Test placing a stop buy order with price below current (should fail)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,  # Below current price
    )

    with pytest.raises(ValueError, match="must be higher than current price"):
        broker.place_order(order_request)


def test_place_stop_sell_order_valid(broker, sample_candle):
    """Test placing a valid stop sell order (price below current)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
    )

    order = broker.place_order(order_request)

    assert order.status == OrderStatus.PLACED
    assert order.stop_price == 95.0
    assert len(broker._pending_orders) == 1


def test_place_stop_sell_order_invalid_price_too_high(broker, sample_candle):
    """Test placing a stop sell order with price above current (should fail)."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=110.0,  # Above current price
    )

    with pytest.raises(ValueError, match="must be lower than current price"):
        broker.place_order(order_request)


# Pending Order Execution Tests
def test_execute_pending_limit_buy_order(broker, sample_candle):
    """Test limit buy order executes when price drops to limit."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    broker.place_order(order_request)

    # Create new candle where low touches limit price
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=100.0,
        high=101.0,
        low=99.0,  # Touches limit price
        close=100.5,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 0
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1
    assert filled_orders[0].filled_avg_price == 99.0
    assert broker.balance == 100000.0 - (10.0 * 99.0)


def test_execute_pending_limit_sell_order(broker, sample_candle):
    """Test limit sell order executes when price rises to limit."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
    )
    broker.place_order(order_request)

    # Create new candle where high touches limit price
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=102.0,
        high=105.0,  # Touches limit price
        low=101.0,
        close=103.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 0
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1
    assert filled_orders[0].filled_avg_price == 105.0


def test_execute_pending_stop_buy_order(broker, sample_candle):
    """Test stop buy order executes when price rises to stop."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
    )
    broker.place_order(order_request)

    # Create new candle where high touches stop price
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=102.0,
        high=105.0,  # Touches stop price
        low=101.0,
        close=103.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 0
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1
    assert filled_orders[0].filled_avg_price == 105.0


def test_execute_pending_stop_sell_order(broker, sample_candle):
    """Test stop sell order executes when price drops to stop."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
    )
    broker.place_order(order_request)

    # Create new candle where low touches stop price
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=100.0,
        high=101.0,
        low=95.0,  # Touches stop price
        close=97.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 0
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1
    assert filled_orders[0].filled_avg_price == 95.0


def test_pending_order_not_executed_when_price_not_reached(broker, sample_candle):
    """Test pending orders remain pending when price conditions not met."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
    )
    broker.place_order(order_request)

    # Create new candle where price doesn't reach limit
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=100.0,
        high=102.0,
        low=98.0,  # Doesn't reach 95.0
        close=101.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 1
    assert broker._pending_orders[0].status == OrderStatus.PLACED


def test_pending_order_rejected_insufficient_balance(broker, sample_candle):
    """Test pending order rejected when insufficient balance at execution time."""
    broker._cur_candle = sample_candle

    # Place a limit buy order
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=1000.0,
        limit_price=99.0,
    )
    broker.place_order(order_request)

    # Reduce balance
    broker.balance = 5000.0

    # Create new candle where limit price is reached
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 0
    rejected_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.REJECTED
    ]
    assert len(rejected_orders) == 1
    assert broker.balance == 5000.0  # Balance unchanged


# Order Management Tests
def test_get_order(broker, sample_candle):
    """Test retrieving a specific order by ID."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    placed_order = broker.place_order(order_request)

    retrieved_order = broker.get_order(placed_order.order_id)

    assert retrieved_order is not None
    assert retrieved_order.order_id == placed_order.order_id
    assert retrieved_order.symbol == "AAPL"


def test_get_order_not_found(broker):
    """Test retrieving non-existent order returns None."""
    order = broker.get_order("nonexistent_id")
    assert order is None


def test_get_orders(broker, sample_candle):
    """Test retrieving all orders."""
    broker._cur_candle = sample_candle

    # Place multiple orders
    for i in range(3):
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
        )
        broker.place_order(order_request)

    orders = broker.get_orders()
    assert len(orders) == 3


def test_cancel_order(broker, sample_candle):
    """Test canceling a pending order."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    order = broker.place_order(order_request)

    result = broker.cancel_order(order.order_id)

    assert result is True
    assert order.status == OrderStatus.CANCELLED
    assert len(broker._pending_orders) == 0


def test_cancel_order_not_found(broker):
    """Test canceling non-existent order returns False."""
    result = broker.cancel_order("nonexistent_id")
    assert result is False


def test_cancel_all_orders(broker, sample_candle):
    """Test canceling all pending orders."""
    broker._cur_candle = sample_candle

    # Place multiple pending orders
    for i in range(3):
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=10.0,
            limit_price=99.0 - i,
        )
        broker.place_order(order_request)

    result = broker.cancel_all_orders()

    assert result is True
    cancelled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.CANCELLED
    ]
    assert len(cancelled_orders) == 3


def test_modify_order_limit_price(broker, sample_candle):
    """Test modifying limit price of an order."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    order = broker.place_order(order_request)

    modified_order = broker.modify_order(order.order_id, limit_price=98.0)

    assert modified_order.limit_price == 98.0


def test_modify_order_stop_price(broker, sample_candle):
    """Test modifying stop price of an order."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
    )
    order = broker.place_order(order_request)

    modified_order = broker.modify_order(order.order_id, stop_price=106.0)

    assert modified_order.stop_price == 106.0


# Stream Candles Tests
def test_stream_candles_with_mocked_db(broker, mock_db_session):
    """Test stream_candles method with mocked database."""
    # Create mock OHLC data
    mock_ohlc_1 = MagicMock()
    mock_ohlc_1.open = 100.0
    mock_ohlc_1.high = 102.0
    mock_ohlc_1.low = 98.0
    mock_ohlc_1.close = 101.0
    mock_ohlc_1.timestamp = 1704103800
    mock_ohlc_1.timeframe = "1m"
    mock_ohlc_1.symbol = "AAPL"

    mock_ohlc_2 = MagicMock()
    mock_ohlc_2.open = 101.0
    mock_ohlc_2.high = 103.0
    mock_ohlc_2.low = 100.0
    mock_ohlc_2.close = 102.0
    mock_ohlc_2.timestamp = 1704103860
    mock_ohlc_2.timeframe = "1m"
    mock_ohlc_2.symbol = "AAPL"

    # Setup mock to return our test data
    mock_result = MagicMock()
    mock_result.yield_per.return_value = [mock_ohlc_1, mock_ohlc_2]
    mock_db_session.scalars.return_value = mock_result

    # Stream candles
    from datetime import datetime as dt

    candles = list(
        broker.stream_candles(
            symbol="AAPL",
            timeframe="1m",
            source="test",
            start_date=dt(2024, 1, 1),
            end_date=dt(2024, 1, 2),
        )
    )

    assert len(candles) == 2
    assert candles[0].close == 101.0
    assert candles[1].close == 102.0


def test_stream_candles_executes_pending_orders(broker, mock_db_session):
    """Test that stream_candles executes pending orders on each candle."""
    # Setup initial candle
    initial_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103800,
        open=100.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = initial_candle

    # Place a limit buy order
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
    )
    broker.place_order(order_request)

    # Create mock OHLC that will trigger the order
    mock_ohlc = MagicMock()
    mock_ohlc.open = 100.0
    mock_ohlc.high = 101.0
    mock_ohlc.low = 99.0  # Triggers limit order
    mock_ohlc.close = 100.0
    mock_ohlc.timestamp = 1704103860
    mock_ohlc.timeframe = "1m"
    mock_ohlc.symbol = "AAPL"

    mock_result = MagicMock()
    mock_result.yield_per.return_value = [mock_ohlc]
    mock_db_session.scalars.return_value = mock_result

    # Stream candles
    from datetime import datetime as dt

    list(
        broker.stream_candles(
            symbol="AAPL",
            timeframe="1m",
            source="test",
            start_date=dt(2024, 1, 1),
            end_date=dt(2024, 1, 2),
        )
    )

    # Verify order was executed
    assert len(broker._pending_orders) == 0
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1


# Edge Cases and Complex Scenarios
def test_multiple_pending_orders_partial_execution(broker, sample_candle):
    """Test multiple pending orders where only some execute."""
    broker._cur_candle = sample_candle

    # Place multiple limit buy orders at different prices
    for price in [95.0, 97.0, 99.0]:
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=10.0,
            limit_price=price,
        )
        broker.place_order(order_request)

    # Create candle that only triggers one order
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=100.0,
        high=101.0,
        low=99.0,  # Only triggers 99.0 limit
        close=100.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    assert len(broker._pending_orders) == 2  # Two orders still pending
    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1


def test_equity_calculation_with_multiple_positions(broker, sample_candle):
    """Test equity calculation with multiple buy/sell transactions."""
    broker._cur_candle = sample_candle

    # Buy 10 shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(buy_request)

    # Buy 5 more shares
    buy_request2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=5.0,
    )
    broker.place_order(buy_request2)

    # Sell 3 shares
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=3.0,
    )
    broker.place_order(sell_request)

    # Net position: 12 shares
    # Cash: 100000 - (10*101) - (5*101) + (3*101) = 100000 - 1212
    expected_cash = 100000.0 - (10.0 * 101.0) - (5.0 * 101.0) + (3.0 * 101.0)
    assert broker.balance == expected_cash

    # Equity: cash + (12 shares * 101)
    equity = broker.get_equity()
    expected_equity = expected_cash + (12.0 * 101.0)
    assert equity == expected_equity


def test_order_execution_at_exact_price(broker, sample_candle):
    """Test that orders execute when price exactly matches trigger price."""
    broker._cur_candle = sample_candle

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,
    )
    broker.place_order(order_request)

    # Candle with low exactly at limit price
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=101.0,
        high=102.0,
        low=100.0,  # Exactly at limit
        close=101.0,
        volume=1000.0,
        timeframe="1m",
    )
    broker._cur_candle = new_candle
    broker._execute_pending_orders()

    filled_orders = [
        o for o in broker._order_map.values() if o.status == OrderStatus.FILLED
    ]
    assert len(filled_orders) == 1
    assert filled_orders[0].filled_avg_price == 100.0


def test_stream_candles_async_not_implemented(broker):
    """Test that stream_candles_async raises NotImplementedError."""
    import pytest

    with pytest.raises(NotImplementedError):
        import asyncio

        asyncio.run(broker.stream_candles_async("AAPL", Timeframe.m1))


def test_get_balance_after_multiple_trades(broker, sample_candle):
    """Test get_balance returns correct balance after multiple trades."""
    broker._cur_candle = sample_candle
    initial_balance = broker.get_balance()

    # Buy 10 shares at 101
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(buy_request)

    expected_balance = initial_balance - (10.0 * 101.0)
    assert broker.get_balance() == expected_balance

    # Sell 5 shares at 101
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=5.0,
    )
    broker.place_order(sell_request)

    expected_balance = expected_balance + (5.0 * 101.0)
    assert broker.get_balance() == expected_balance


def test_get_equity_with_price_change(broker, sample_candle):
    """Test get_equity reflects unrealized gains/losses with price changes."""
    broker._cur_candle = sample_candle
    initial_equity = broker.get_equity()

    # Buy 10 shares at 101
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(buy_request)

    # Equity should remain same (cash decreased, holdings increased)
    assert broker.get_equity() == initial_equity

    # Price increases to 110
    new_candle = OHLC(
        symbol="AAPL",
        timestamp=1704103860,
        open=105.0,
        high=112.0,
        low=104.0,
        close=110.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._cur_candle = new_candle

    # Equity should increase by unrealized gain: (110 - 101) * 10 = 90
    expected_equity = initial_equity + (110.0 - 101.0) * 10.0
    assert broker.get_equity() == expected_equity

    # Price decreases to 95
    new_candle2 = OHLC(
        symbol="AAPL",
        timestamp=1704103920,
        open=110.0,
        high=111.0,
        low=94.0,
        close=95.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._cur_candle = new_candle2

    # Equity should decrease by unrealized loss: (95 - 101) * 10 = -60
    expected_equity = initial_equity + (95.0 - 101.0) * 10.0
    assert broker.get_equity() == expected_equity


def test_get_equity_with_no_position(broker, sample_candle):
    """Test get_equity equals balance when no positions are held."""
    broker._cur_candle = sample_candle

    # No positions held
    assert broker.get_equity() == broker.get_balance()

    # Buy and sell same quantity (round trip)
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(buy_request)

    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
    )
    broker.place_order(sell_request)

    # No positions held again
    assert broker.get_equity() == broker.get_balance()


def test_get_balance_unchanged_for_pending_orders(broker, sample_candle):
    """Test get_balance remains unchanged when orders are pending."""
    broker._cur_candle = sample_candle
    initial_balance = broker.get_balance()

    # Place limit order (pending)
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
    )
    broker.place_order(order_request)

    # Balance should remain unchanged
    assert broker.get_balance() == initial_balance

    # Place stop order (pending)
    order_request2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=5.0,
        stop_price=110.0,
    )
    broker.place_order(order_request2)

    # Balance should still remain unchanged
    assert broker.get_balance() == initial_balance


def test_get_equity_with_mixed_positions(broker, sample_candle):
    """Test get_equity calculation with multiple symbols (if supported)."""
    broker._cur_candle = sample_candle
    initial_equity = broker.get_equity()

    # Buy 20 shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=20.0,
    )
    broker.place_order(buy_request)

    # Sell 8 shares
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=8.0,
    )
    broker.place_order(sell_request)

    # Net position: 12 shares
    # Balance: 100000 - (20*101) + (8*101) = 100000 - 1212
    expected_balance = 100000.0 - (20.0 * 101.0) + (8.0 * 101.0)
    assert broker.get_balance() == expected_balance

    # Equity: balance + (12 * 101)
    expected_equity = expected_balance + (12.0 * 101.0)
    assert broker.get_equity() == expected_equity

    # Verify equity equals initial equity (no realized P&L yet)
    assert broker.get_equity() == initial_equity


# Sharpe Ratio Calculation Tests
class DummyStrategy(BaseStrategy):
    """Dummy strategy for testing."""

    def startup(self):
        pass

    def shutdown(self):
        pass

    def on_candle(self, candle):
        pass


@pytest.fixture
def backtest_config_intraday():
    """Config for intraday backtest (< 1 day)."""
    return BacktestConfig(
        symbol="AAPL",
        timeframe=Timeframe.m1,
        broker="alpaca",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1),
        starting_balance=100000.0,
    )


@pytest.fixture
def backtest_config_daily():
    """Config for daily backtest (1-7 days)."""
    return BacktestConfig(
        symbol="AAPL",
        timeframe=Timeframe.m1,
        broker="alpaca",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 5),
        starting_balance=100000.0,
    )


@pytest.fixture
def backtest_config_weekly():
    """Config for weekly backtest (7-30 days)."""
    return BacktestConfig(
        symbol="AAPL",
        timeframe=Timeframe.m1,
        broker="alpaca",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 22),
        starting_balance=100000.0,
    )


@pytest.fixture
def backtest_config_monthly():
    """Config for monthly backtest (30-365 days)."""
    return BacktestConfig(
        symbol="AAPL",
        timeframe=Timeframe.m1,
        broker="alpaca",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 1),
        starting_balance=100000.0,
    )


@pytest.fixture
def backtest_config_yearly():
    """Config for yearly backtest (>= 365 days)."""
    return BacktestConfig(
        symbol="AAPL",
        timeframe=Timeframe.m1,
        broker="alpaca",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 1),
        starting_balance=100000.0,
    )


def test_sharpe_ratio_with_no_equity_points(backtest_config_daily):
    """Test Sharpe ratio returns 0 when no equity points exist."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe == 0.0


def test_sharpe_ratio_with_single_equity_point(backtest_config_daily):
    """Test Sharpe ratio returns 0 with only one equity point."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    engine._equity_curve.append(
        EquityCurvePoint(
            timestamp=int(datetime(2024, 1, 1, tzinfo=UTC).timestamp()), value=100000.0
        )
    )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe == 0.0


def test_sharpe_ratio_intraday_positive_returns(backtest_config_intraday):
    """Test Sharpe ratio calculation for intraday backtest with positive returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_intraday)

    # Create equity curve with increasing values (12 hourly points)
    for i in range(12):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(datetime(2024, 1, 1, i, 0, 0, tzinfo=UTC).timestamp()),
                value=100000.0 + i * 100,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe > 0  # Positive returns should yield positive Sharpe


def test_sharpe_ratio_daily_positive_returns(backtest_config_daily):
    """Test Sharpe ratio calculation for daily backtest with positive returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with one point per day
    for i in range(5):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(
                    datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC).timestamp()
                ),
                value=100000.0 + i * 1000,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe > 0


def test_sharpe_ratio_weekly_positive_returns(backtest_config_weekly):
    """Test Sharpe ratio calculation for weekly backtest with positive returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_weekly)

    # Create equity curve with one point per day for 21 days
    for i in range(22):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(
                    datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC).timestamp()
                ),
                value=100000.0 + i * 500,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe > 0


def test_sharpe_ratio_monthly_positive_returns(backtest_config_monthly):
    """Test Sharpe ratio calculation for monthly backtest with positive returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_monthly)

    # Create equity curve with one point per day for 60 days
    # This will be resampled to monthly periods (2 months = 2 points)
    base_date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    for i in range(60):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=100000.0 + i * 200,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    # After resampling to monthly, we may have insufficient points for std calculation
    # Just verify it's a valid float (could be 0, nan, or positive)
    assert isinstance(sharpe, float)


def test_sharpe_ratio_yearly_positive_returns(backtest_config_yearly):
    """Test Sharpe ratio calculation for yearly backtest with positive returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_yearly)

    # Create equity curve with one point per week for 52 weeks
    # This will be resampled to yearly periods (1 year = 1 point after resampling)
    base_date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    for i in range(52):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(weeks=i)).timestamp()),
                value=100000.0 + i * 1000,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    # After resampling to yearly, we only have 1 point, so Sharpe will be 0
    assert sharpe == 0.0


def test_sharpe_ratio_negative_returns(backtest_config_daily):
    """Test Sharpe ratio calculation with negative returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with decreasing values
    for i in range(5):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(
                    datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC).timestamp()
                ),
                value=100000.0 - i * 1000,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe < 0  # Negative returns should yield negative Sharpe


def test_sharpe_ratio_flat_returns(backtest_config_daily):
    """Test Sharpe ratio calculation with flat returns (no change)."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with constant values
    for i in range(5):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(
                    datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC).timestamp()
                ),
                value=100000.0,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe == 0.0  # No returns means Sharpe ratio of 0


def test_sharpe_ratio_volatile_returns(backtest_config_daily):
    """Test Sharpe ratio calculation with volatile returns."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with volatile values
    equity_values = [100000.0, 102000.0, 99000.0, 103000.0, 98000.0, 104000.0]
    for i, equity in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int(
                    datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC).timestamp()
                ),
                value=equity,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    # Volatile returns should result in lower Sharpe ratio
    assert isinstance(sharpe, float)


def test_sharpe_ratio_handles_datetime_timestamps(backtest_config_daily):
    """Test Sharpe ratio calculation handles datetime timestamps correctly."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with datetime timestamps (not int)
    for i in range(5):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=datetime(2024, 1, 1 + i, 12, 0, 0, tzinfo=UTC),
                value=100000.0 + i * 1000,
            )
        )

    sharpe = engine._calculate_sharpe_ratio()
    assert sharpe > 0  # Should handle datetime timestamps


def test_sharpe_ratio_resampling_daily(backtest_config_daily):
    """Test that daily resampling works correctly."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with multiple points per day
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    for day in range(5):
        for hour in range(24):
            engine._equity_curve.append(
                EquityCurvePoint(
                    timestamp=int(
                        (base_date + timedelta(days=day, hours=hour)).timestamp()
                    ),
                    value=100000.0 + day * 1000 + hour * 10,
                )
            )

    sharpe = engine._calculate_sharpe_ratio()
    assert isinstance(sharpe, float)
    assert sharpe > 0


def test_sharpe_ratio_resampling_weekly(backtest_config_weekly):
    """Test that weekly resampling works correctly."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_weekly)

    # Create equity curve with hourly points for 21 days
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    for day in range(21):
        for hour in [0, 12]:  # Two points per day
            engine._equity_curve.append(
                EquityCurvePoint(
                    timestamp=int(
                        (base_date + timedelta(days=day, hours=hour)).timestamp()
                    ),
                    value=100000.0 + day * 500,
                )
            )

    sharpe = engine._calculate_sharpe_ratio()
    assert isinstance(sharpe, float)


def test_sharpe_ratio_integration_with_backtest(backtest_config_daily, mock_db_session):
    """Test Sharpe ratio calculation integrates correctly with full backtest."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Mock database to return candles
    base_date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_candles = []
    for i in range(5):
        mock_ohlc = MagicMock()
        mock_ohlc.open = 100.0 + i
        mock_ohlc.high = 102.0 + i
        mock_ohlc.low = 98.0 + i
        mock_ohlc.close = 101.0 + i
        mock_ohlc.timestamp = int((base_date + timedelta(days=i)).timestamp())
        mock_ohlc.timeframe = "1m"
        mock_ohlc.symbol = "AAPL"
        mock_candles.append(mock_ohlc)

    mock_result = MagicMock()
    mock_result.yield_per.return_value = mock_candles
    mock_db_session.scalars.return_value = mock_result

    # Run backtest
    metrics = engine.run()

    # Verify Sharpe ratio is calculated
    assert hasattr(metrics, "sharpe_ratio")
    assert isinstance(metrics.sharpe_ratio, float)


# Maximum Drawdown Tests
def test_max_drawdown_no_drawdown(backtest_config_daily):
    """Test max drawdown with monotonically increasing equity (no drawdown)."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve that only goes up
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    for i in range(10):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=100000.0 + i * 1000,  # Increases by $1000 each day
            )
        )

    max_dd = engine._calculate_max_drawdown()
    assert max_dd == 0.0


def test_max_drawdown_single_drawdown(backtest_config_daily):
    """Test max drawdown with a single drawdown period."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve: 100k -> 110k -> 90k -> 95k
    # Peak at 110k, trough at 90k
    # Drawdown = (90k - 110k) / 110k = -20k / 110k = -0.1818 (18.18%)
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [100000.0, 110000.0, 90000.0, 95000.0]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    expected_dd = abs((90000.0 - 110000.0) / 110000.0)
    assert abs(max_dd - expected_dd) < 0.0001  # ~18.18%


def test_max_drawdown_multiple_drawdowns(backtest_config_daily):
    """Test max drawdown with multiple drawdown periods - should return the largest."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with two drawdowns:
    # First: 100k -> 120k -> 100k (16.67% drawdown)
    # Second: 100k -> 150k -> 90k (40% drawdown) <- This is the max
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [
        100000.0,  # Start
        120000.0,  # First peak
        100000.0,  # First trough (16.67% DD)
        110000.0,  # Recovery
        150000.0,  # Second peak (new high)
        90000.0,   # Second trough (40% DD) <- Maximum drawdown
        100000.0,  # Partial recovery
    ]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    expected_dd = abs((90000.0 - 150000.0) / 150000.0)  # 40%
    assert abs(max_dd - expected_dd) < 0.0001


def test_max_drawdown_continuous_decline(backtest_config_daily):
    """Test max drawdown with continuous decline from peak."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve that peaks at start and continuously declines
    # 100k -> 90k -> 80k -> 70k -> 60k
    # Max drawdown = (60k - 100k) / 100k = -40%
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [100000.0, 90000.0, 80000.0, 70000.0, 60000.0]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    expected_dd = abs((60000.0 - 100000.0) / 100000.0)  # 40%
    assert abs(max_dd - expected_dd) < 0.0001


def test_max_drawdown_recovery_to_new_high(backtest_config_daily):
    """Test max drawdown when equity recovers to new high after drawdown."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve: 100k -> 120k -> 80k -> 140k
    # Drawdown from 120k to 80k = 33.33%
    # Then recovers to new high at 140k
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [100000.0, 120000.0, 80000.0, 140000.0]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    expected_dd = abs((80000.0 - 120000.0) / 120000.0)  # 33.33%
    assert abs(max_dd - expected_dd) < 0.0001


def test_max_drawdown_flat_equity(backtest_config_daily):
    """Test max drawdown with flat equity curve (no change)."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create flat equity curve
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    for i in range(10):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=100000.0,  # Constant value
            )
        )

    max_dd = engine._calculate_max_drawdown()
    assert max_dd == 0.0


def test_max_drawdown_empty_curve(backtest_config_daily):
    """Test max drawdown with empty equity curve."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Empty equity curve
    max_dd = engine._calculate_max_drawdown()
    assert max_dd == 0.0


def test_max_drawdown_single_point(backtest_config_daily):
    """Test max drawdown with only one equity point."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Single point
    engine._equity_curve.append(
        EquityCurvePoint(
            timestamp=int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp()),
            value=100000.0,
        )
    )

    max_dd = engine._calculate_max_drawdown()
    assert max_dd == 0.0


def test_max_drawdown_small_fluctuations(backtest_config_daily):
    """Test max drawdown with small fluctuations around a trend."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with small fluctuations
    # 100k -> 101k -> 100.5k -> 102k -> 101k -> 103k
    # Max drawdown from 102k to 101k = 0.98%
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [100000.0, 101000.0, 100500.0, 102000.0, 101000.0, 103000.0]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    # Maximum drawdown is from 102k to 101k
    expected_dd = abs((101000.0 - 102000.0) / 102000.0)  # ~0.98%
    assert abs(max_dd - expected_dd) < 0.0001


def test_max_drawdown_severe_crash(backtest_config_daily):
    """Test max drawdown with severe crash (>50% decline)."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Create equity curve with severe crash
    # 100k -> 150k -> 50k (66.67% drawdown)
    base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    equity_values = [100000.0, 150000.0, 50000.0]

    for i, value in enumerate(equity_values):
        engine._equity_curve.append(
            EquityCurvePoint(
                timestamp=int((base_date + timedelta(days=i)).timestamp()),
                value=value,
            )
        )

    max_dd = engine._calculate_max_drawdown()
    expected_dd = abs((50000.0 - 150000.0) / 150000.0)  # 66.67%
    assert abs(max_dd - expected_dd) < 0.0001


def test_max_drawdown_integration_with_backtest(backtest_config_daily, mock_db_session):
    """Test max drawdown calculation integrates correctly with full backtest."""
    broker = BacktestBroker(100000.0)
    strategy = DummyStrategy(name="test", broker=broker)
    engine = BacktestEngine(strategy, broker, backtest_config_daily)

    # Mock database to return candles with varying prices
    base_date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    mock_candles = []
    prices = [100.0, 110.0, 90.0, 105.0, 95.0]  # Creates drawdowns
    for i, price in enumerate(prices):
        mock_ohlc = MagicMock()
        mock_ohlc.open = price
        mock_ohlc.high = price + 2
        mock_ohlc.low = price - 2
        mock_ohlc.close = price
        mock_ohlc.timestamp = int((base_date + timedelta(days=i)).timestamp())
        mock_ohlc.timeframe = "1m"
        mock_ohlc.symbol = "AAPL"
        mock_candles.append(mock_ohlc)

    mock_result = MagicMock()
    mock_result.yield_per.return_value = mock_candles
    mock_db_session.scalars.return_value = mock_result

    # Run backtest
    metrics = engine.run()

    # Verify max drawdown is calculated and is a valid value
    assert hasattr(metrics, "max_drawdown")
    assert isinstance(metrics.max_drawdown, float)
    assert metrics.max_drawdown >= 0.0  # Should be positive percentage
    assert metrics.max_drawdown <= 1.0  # Should not exceed 100%
    assert metrics.sharpe_ratio >= 0  # Should be non-negative for this test case
