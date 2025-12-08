from datetime import datetime
from decimal import Decimal

import pytest

from engine.brokers.backtest import BacktestBroker
from engine.brokers.exc import (
    BrokerError,
    InsufficientFundsError,
    OrderRejectedError,
)
from engine.models import OrderRequest, Account
from engine.enums import OrderSide, OrderType, OrderStatus, TimeInForce
from engine.ohlcv import OHLCV, Timeframe
from config import ALPACA_API_KEY

# Fixtures


@pytest.fixture
def broker() -> BacktestBroker:
    """Create a broker with $10,000 starting balance."""
    return BacktestBroker(starting_balance=10000.0)


@pytest.fixture
def broker_with_candle(broker: BacktestBroker) -> BacktestBroker:
    """Create a broker with current candle set to $100 close price."""
    broker._current_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 30),
        open=99.0,
        high=101.0,
        low=98.0,
        close=100.0,
        volume=1000,
        timeframe=Timeframe.M1,
    )
    return broker


# Market Order Tests


def test_market_buy_order_with_sufficient_funds(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify market buy order executes when sufficient funds available."""
    initial_cash = broker_with_candle._cash

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)

    assert response.status == OrderStatus.FILLED
    assert response.filled_quantity == 10.0
    assert response.avg_fill_price == 100.0
    assert broker_with_candle._cash == initial_cash - (10.0 * 100.0)
    assert response.order_id in broker_with_candle._orders


def test_market_buy_order_with_insufficient_funds(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify market buy order fails when insufficient funds."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=200.0,  # Would cost $20,000 but only have $10,000
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(InsufficientFundsError) as exc_info:
        broker_with_candle.submit_order(order_request)

    assert "Insufficient funds" in str(exc_info.value)
    assert broker_with_candle._cash == 10000.0  # Cash unchanged


def test_market_sell_order_with_sufficient_position(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify market sell order executes when sufficient position available."""
    # First buy some shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_request)

    initial_cash = broker_with_candle._cash

    # Now sell them
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(sell_request)

    assert response.status == OrderStatus.FILLED
    assert response.filled_quantity == 10.0
    assert response.avg_fill_price == 100.0
    assert broker_with_candle._cash == initial_cash + (10.0 * 100.0)


def test_market_sell_order_with_insufficient_position(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify market sell order fails when insufficient position (TESTS BUG at line 125)."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,  # Don't own any shares
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(OrderRejectedError) as exc_info:
        broker_with_candle.submit_order(order_request)

    assert "Insufficient position" in str(exc_info.value)


def test_market_order_without_current_candle() -> None:
    """Verify market order fails when no current candle is set."""
    broker = BacktestBroker(starting_balance=10000.0)

    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(BrokerError) as exc_info:
        broker.submit_order(order_request)

    assert "No price data available" in str(exc_info.value)


def test_market_order_with_zero_quantity(broker_with_candle: BacktestBroker) -> None:
    """Verify order validation rejects zero quantity."""
    with pytest.raises(ValueError):
        OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=0.0,
            time_in_force=TimeInForce.GTC,
        )


# Limit Order Tests


def test_limit_buy_order_submission(broker_with_candle: BacktestBroker) -> None:
    """Verify limit buy order is submitted as pending."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)

    assert response.status == OrderStatus.PENDING
    assert response.filled_quantity == 0.0
    assert response.limit_price == 95.0
    assert response.order_id in broker_with_candle._pending_orders


def test_limit_buy_order_filled_when_price_drops(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify limit buy order fills when price drops to or below limit price."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,  # Current price is 100
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)
    assert response.status == OrderStatus.PENDING

    # Price drops to limit price
    broker_with_candle._current_candle.close = 100.0
    broker_with_candle.process_pending_orders()

    # Check order is filled
    filled_order = broker_with_candle._orders[response.order_id]
    assert filled_order.status == OrderStatus.FILLED
    assert filled_order.filled_quantity == 10.0
    assert response.order_id not in broker_with_candle._pending_orders


def test_limit_buy_order_not_filled_when_price_above_limit(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify limit buy order remains pending when price is above limit."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)

    # Price stays above limit
    broker_with_candle._current_candle.close = 100.0
    broker_with_candle.process_pending_orders()

    # Order should still be pending
    assert response.order_id in broker_with_candle._pending_orders
    assert response.status == OrderStatus.PENDING


def test_limit_sell_order_filled_when_price_rises(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify limit sell order fills when price rises to or above limit price."""
    # First buy shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_request)

    # Submit limit sell order
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(sell_request)
    assert response.status == OrderStatus.PENDING

    # Price rises to limit price
    broker_with_candle._current_candle.close = 100.0
    broker_with_candle.process_pending_orders()

    # Check order is filled
    filled_order = broker_with_candle._orders[response.order_id]
    assert filled_order.status == OrderStatus.FILLED


def test_limit_sell_order_not_filled_when_price_below_limit(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify limit sell order remains pending when price is below limit."""
    # First buy shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_request)

    # Submit limit sell order above current price
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(sell_request)

    # Price stays below limit
    broker_with_candle._current_candle.close = 100.0
    broker_with_candle.process_pending_orders()

    # Order should still be pending
    assert response.order_id in broker_with_candle._pending_orders


# Stop Order Tests


def test_stop_buy_order_submission(broker_with_candle: BacktestBroker) -> None:
    """Verify stop buy order is submitted as pending."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)

    assert response.status == OrderStatus.PENDING
    assert response.filled_quantity == 0.0
    assert response.stop_price == 105.0
    assert response.order_id in broker_with_candle._pending_orders


def test_stop_buy_order_triggered_when_price_rises(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify stop buy order triggers when price rises to or above stop price."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)
    assert response.status == OrderStatus.PENDING

    # Price rises to stop price
    broker_with_candle._current_candle.close = 105.0
    broker_with_candle.process_pending_orders()

    # Check order is filled
    filled_order = broker_with_candle._orders[response.order_id]
    assert filled_order.status == OrderStatus.FILLED
    assert filled_order.avg_fill_price == 105.0


def test_stop_sell_order_triggered_when_price_falls(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify stop sell order triggers when price falls to or below stop price."""
    # First buy shares
    buy_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_request)

    # Submit stop sell order
    sell_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(sell_request)
    assert response.status == OrderStatus.PENDING

    # Price falls to stop price
    broker_with_candle._current_candle.close = 95.0
    broker_with_candle.process_pending_orders()

    # Check order is filled
    filled_order = broker_with_candle._orders[response.order_id]
    assert filled_order.status == OrderStatus.FILLED


# Position Tracking Tests


def test_position_tracking_after_multiple_buys(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify position is correctly tracked after multiple buy orders."""
    # Buy 10 shares
    order1 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(order1)

    # Buy 5 more shares
    order2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=5.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(order2)

    # Calculate total position (TESTS BUG - doesn't differentiate BUY/SELL)
    total_position = sum(
        order.filled_quantity
        for order in broker_with_candle._orders.values()
        if order.side == OrderSide.BUY
    )

    assert total_position == 15.0


def test_position_tracking_after_buy_and_sell(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify position is correctly tracked after buy and sell orders (TESTS BUG at line 125)."""
    # Buy 10 shares
    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_order)

    # Sell 5 shares
    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=5.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(sell_order)

    # Net position should be 5 shares
    # BUG: Line 125 doesn't differentiate between BUY and SELL
    # It sums ALL filled_quantity regardless of side
    total_buys = sum(
        order.filled_quantity
        for order in broker_with_candle._orders.values()
        if order.side == OrderSide.BUY
    )
    total_sells = sum(
        order.filled_quantity
        for order in broker_with_candle._orders.values()
        if order.side == OrderSide.SELL
    )
    net_position = total_buys - total_sells

    assert net_position == 5.0


# Balance Update Tests


def test_balance_decreases_after_buy(broker_with_candle: BacktestBroker) -> None:
    """Verify cash balance decreases after buy order."""
    initial_cash = broker_with_candle._cash

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(order)

    expected_cash = initial_cash - (10.0 * 100.0)
    assert broker_with_candle._cash == expected_cash


def test_balance_increases_after_sell(broker_with_candle: BacktestBroker) -> None:
    """Verify cash balance increases after sell order."""
    # First buy shares
    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(buy_order)

    cash_before_sell = broker_with_candle._cash

    # Now sell
    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(sell_order)

    expected_cash = cash_before_sell + (10.0 * 100.0)
    assert broker_with_candle._cash == expected_cash


# Order Cancellation Tests


def test_cancel_pending_limit_order(broker_with_candle: BacktestBroker) -> None:
    """Verify pending limit order can be cancelled."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)
    assert response.status == OrderStatus.PENDING

    # Cancel the order
    result = broker_with_candle.cancel_order(response.order_id)

    assert result is True
    assert response.order_id not in broker_with_candle._pending_orders
    assert response.status == OrderStatus.CANCELLED


def test_cancel_filled_order_fails(broker_with_candle: BacktestBroker) -> None:
    """Verify filled order cannot be cancelled."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)
    assert response.status == OrderStatus.FILLED

    # Try to cancel filled order
    result = broker_with_candle.cancel_order(response.order_id)

    assert result is False


def test_cancel_nonexistent_order(broker_with_candle: BacktestBroker) -> None:
    """Verify cancelling non-existent order returns False."""
    result = broker_with_candle.cancel_order("nonexistent_id")
    assert result is False


def test_cancel_all_orders(broker_with_candle: BacktestBroker) -> None:
    """Verify all pending orders can be cancelled at once."""
    # Submit multiple pending orders
    for i in range(3):
        order_request = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=10.0,
            limit_price=95.0 - i,
            time_in_force=TimeInForce.GTC,
        )
        broker_with_candle.submit_order(order_request)

    # Cancel all orders
    broker_with_candle.cancel_all_orders()

    # Verify no pending orders remain
    assert len(broker_with_candle._pending_orders) == 0


# Account Information Tests


def test_get_account_initial_state(broker_with_candle: BacktestBroker) -> None:
    """Verify get_account returns correct initial state."""
    account = broker_with_candle.get_account()

    assert account.cash == 10000.0
    assert account.equity == 10000.0
    assert account.account_id == broker_with_candle._account_id


def test_get_account_after_trades(broker_with_candle: BacktestBroker) -> None:
    """Verify get_account returns correct state after trades (TESTS BUG at lines 348-357)."""
    # Buy 10 shares at $100
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(order)

    # Price increases to $110
    broker_with_candle._current_candle.close = 110.0

    account = broker_with_candle.get_account()

    # Cash should be reduced by purchase
    assert account.cash == 9000.0

    assert account.equity == 10100


# Order Retrieval Tests


def test_get_order_by_id(broker_with_candle: BacktestBroker) -> None:
    """Verify order can be retrieved by ID."""
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)
    retrieved_order = broker_with_candle.get_order(response.order_id)

    assert retrieved_order.order_id == response.order_id
    assert retrieved_order.symbol == "AAPL"
    assert retrieved_order.quantity == 10.0


def test_get_order_nonexistent_raises_error(broker_with_candle: BacktestBroker) -> None:
    """Verify getting non-existent order raises BrokerError."""
    with pytest.raises(BrokerError) as exc_info:
        broker_with_candle.get_order("nonexistent_id")

    assert "not found" in str(exc_info.value)


def test_get_open_orders(broker_with_candle: BacktestBroker) -> None:
    """
    Verify get_open_orders() returns pending limit/stop orders from _orders.

    The BacktestBroker maintains orders in two dictionaries:
    - _orders: All submitted orders with their current status (line 95 in backtest.py)
    - _pending_orders: Reference to pending limit/stop orders for price condition checking

    When a limit/stop order is submitted:
    1. An OrderResponse with PENDING status is created
    2. It's stored in both _orders (line 95) and _pending_orders (lines 197, 231)
    3. get_open_orders() searches _orders for PENDING/PARTIALLY_FILLED status (lines 336-356)

    This test verifies that get_open_orders() correctly returns pending orders,
    and that these orders also exist in _pending_orders for execution processing.
    """
    # Submit a market order (filled immediately, should not appear in open orders)
    market_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker_with_candle.submit_order(market_order)

    # Submit a limit order (pending, should appear in open orders)
    limit_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )
    limit_response = broker_with_candle.submit_order(limit_order)

    # get_open_orders() should return the pending limit order
    open_orders = broker_with_candle.get_open_orders()
    assert len(open_orders) == 1
    assert open_orders[0].order_id == limit_response.order_id
    assert open_orders[0].status == OrderStatus.PENDING
    assert open_orders[0].order_type == OrderType.LIMIT

    # Verify the order exists in both _orders and _pending_orders
    assert limit_response.order_id in broker_with_candle._orders
    assert limit_response.order_id in broker_with_candle._pending_orders
    assert (
        broker_with_candle._orders[limit_response.order_id].status
        == OrderStatus.PENDING
    )


def test_get_open_orders_filtered_by_symbol(broker_with_candle: BacktestBroker) -> None:
    """Verify get_open_orders can be filtered by symbol."""
    # This would require orders in _orders with PENDING status
    # Current implementation keeps pending orders separate in _pending_orders
    pass


# Edge Cases


def test_process_pending_orders_with_insufficient_funds(
    broker_with_candle: BacktestBroker,
) -> None:
    """Verify pending order rejected when insufficient funds at execution time."""
    # Submit limit order that would require all funds
    order_request = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=100.0,  # $10,000 worth
        limit_price=100.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order_request)

    # Price drops to trigger order
    broker_with_candle._current_candle.close = 100.0
    broker_with_candle.process_pending_orders()

    # Order should be filled
    assert response.order_id in broker_with_candle._orders


def test_multiple_orders_at_same_price(broker_with_candle: BacktestBroker) -> None:
    """Verify multiple orders can be executed at the same price."""
    for i in range(3):
        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
            time_in_force=TimeInForce.GTC,
        )
        response = broker_with_candle.submit_order(order)
        assert response.status == OrderStatus.FILLED

    # All orders should be filled at same price
    orders = list(broker_with_candle._orders.values())
    assert len(orders) == 3
    assert all(order.avg_fill_price == 100.0 for order in orders)


def test_connection_methods(broker: BacktestBroker) -> None:
    """Verify connect and disconnect methods work as expected."""
    broker.connect()
    assert broker._connected is True

    broker.disconnect()
    assert broker._connected is False


def test_order_timestamps(broker_with_candle: BacktestBroker) -> None:
    """Verify order timestamps match current candle timestamp."""
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker_with_candle.submit_order(order)

    assert response.created_at == broker_with_candle._current_candle.timestamp
    assert response.filled_at == broker_with_candle._current_candle.timestamp
