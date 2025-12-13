import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from engine.brokers.backtest import BacktestBroker
from engine.models import (
    OrderRequest,
    OrderSide,
    OrderType,
    OrderStatus,
    TimeInForce,
)
from engine.ohlcv import OHLCV
from engine.enums import Timeframe
from engine.brokers.exc import BrokerError, OrderRejectedError


@pytest.fixture
def broker():
    return BacktestBroker(starting_balance=100000.0)


@pytest.fixture
def sample_candle():
    return OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 30),
        open=100.0,
        high=102.0,
        low=98.0,
        close=101.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )


def test_broker_initialization(broker):
    assert broker._cash == 100000.0
    assert broker._orders == {}
    assert broker._pending_orders == {}
    assert broker._current_candle is None


def test_broker_connect(broker):
    broker.connect()
    assert broker._connected is True


def test_broker_disconnect(broker):
    broker.connect()
    broker.disconnect()
    assert broker._connected is False


def test_submit_market_buy_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.FILLED
    assert response.filled_quantity == 10.0
    assert response.avg_fill_price == 101.0
    assert broker._cash == 100000.0 - (sample_candle.close * 10.0)


def test_submit_market_sell_order(broker, sample_candle):
    broker._current_candle = sample_candle

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(buy_order)

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(sell_order)

    assert response.status == OrderStatus.FILLED
    assert response.filled_quantity == 10.0


def test_submit_market_order_insufficient_funds(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=2000.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED


def test_submit_market_order_insufficient_position(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED


def test_submit_market_order_no_candle_data(broker):
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    with pytest.raises(BrokerError, match="No price data available"):
        broker.submit_order(order)


def test_submit_limit_buy_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.PENDING
    assert response.limit_price == 99.0
    assert response.filled_quantity == 0.0
    assert len(broker._pending_orders) == 1


def test_submit_limit_buy_order_rejected_price_too_high(broker, sample_candle):
    """Limit buy orders should be rejected if limit price is above current price"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED
    assert len(broker._pending_orders) == 0


def test_submit_limit_sell_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.PENDING
    assert response.limit_price == 105.0
    assert len(broker._pending_orders) == 1


def test_submit_limit_sell_order_rejected_price_too_low(broker, sample_candle):
    """Limit sell orders should be rejected if limit price is below current price"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED
    assert len(broker._pending_orders) == 0


def test_submit_stop_buy_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.PENDING
    assert response.stop_price == 105.0
    assert len(broker._pending_orders) == 1


def test_submit_stop_buy_order_rejected_price_too_low(broker, sample_candle):
    """Stop buy orders should be rejected if stop price is below current price"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED
    assert len(broker._pending_orders) == 0


def test_submit_stop_sell_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.PENDING
    assert response.stop_price == 95.0


def test_submit_stop_sell_order_rejected_price_too_high(broker, sample_candle):
    """Stop sell orders should be rejected if stop price is above current price"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=110.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED
    assert len(broker._pending_orders) == 0


def test_process_pending_limit_buy_order_filled(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=101.0,
        high=103.0,
        low=100.0,
        close=90.5,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0
    assert any(o.status == OrderStatus.FILLED for o in broker._orders.values())


def test_process_pending_limit_sell_order_filled(broker, sample_candle):
    broker._current_candle = sample_candle

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(buy_order)

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=105.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(sell_order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.5,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_process_pending_stop_buy_order_triggered(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=104.0,
        high=107.0,
        low=103.0,
        close=106.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_process_pending_stop_sell_order_triggered(broker, sample_candle):
    broker._current_candle = sample_candle

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(buy_order)

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=95.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(sell_order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=100.0,
        high=101.0,
        low=93.0,
        close=94.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_cancel_pending_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)
    result = broker.cancel_order(response.order_id)

    assert result is True
    assert len(broker._pending_orders) == 0
    assert response.status == OrderStatus.CANCELLED


def test_cancel_nonexistent_order(broker):
    result = broker.cancel_order("nonexistent_id")
    assert result is False


def test_get_order(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)
    retrieved_order = broker.get_order(response.order_id)

    assert retrieved_order.order_id == response.order_id
    assert retrieved_order.symbol == "AAPL"


def test_get_order_not_found(broker):
    with pytest.raises(BrokerError, match="Order .* not found"):
        broker.get_order("nonexistent_id")


def test_get_open_orders(broker, sample_candle):
    broker._current_candle = sample_candle

    order1 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
        time_in_force=TimeInForce.GTC,
    )

    order2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=5.0,
        limit_price=98.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order1)
    broker.submit_order(order2)

    open_orders = broker.get_open_orders()

    assert len(open_orders) == 2


def test_get_open_orders_filtered_by_symbol(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order)

    open_orders = broker.get_open_orders(symbol="AAPL")

    assert len(open_orders) == 1
    assert open_orders[0].symbol == "AAPL"


def test_get_account(broker, sample_candle):
    broker._current_candle = sample_candle

    account = broker.get_account()

    assert account.cash == 100000.0
    assert account.equity == 100000.0


def test_get_account_with_position(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order)
    account = broker.get_account()

    assert account.cash == 100000.0 - (101.0 * 10.0)
    assert account.equity == 100000.0


def test_get_account_equity_changes_with_price(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=105.0,
        high=107.0,
        low=104.0,
        close=106.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    account = broker.get_account()

    expected_equity = (100000.0 - 1010.0) + (106.0 * 10.0)
    assert account.equity == expected_equity


def test_cancel_all_orders(broker, sample_candle):
    broker._current_candle = sample_candle

    order1 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=99.0,
        time_in_force=TimeInForce.GTC,
    )

    order2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=5.0,
        limit_price=98.0,
        time_in_force=TimeInForce.GTC,
    )

    broker.submit_order(order1)
    broker.submit_order(order2)

    broker.cancel_all_orders()

    open_orders = broker.get_open_orders()
    assert len(open_orders) == 0


def test_market_order_with_notional(broker, sample_candle):
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        # quantity=0.0,
        notional=10000.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.FILLED
    assert broker._cash == 100000.0 - 10000.0


def test_multiple_consecutive_buys(broker, sample_candle):
    """Test multiple buy orders in sequence"""
    broker._current_candle = sample_candle

    for i in range(3):
        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.0,
            time_in_force=TimeInForce.GTC,
        )
        response = broker.submit_order(order)
        assert response.status == OrderStatus.FILLED

    account = broker.get_account()
    assert account.cash == 100000.0 - (101.0 * 30.0)


def test_partial_position_sell(broker, sample_candle):
    """Test selling only part of a position"""
    broker._current_candle = sample_candle

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=20.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(buy_order)

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    response = broker.submit_order(sell_order)

    assert response.status == OrderStatus.FILLED
    assert response.filled_quantity == 10.0


def test_round_trip_trade(broker, sample_candle):
    """Test complete buy and sell cycle"""
    broker._current_candle = sample_candle
    initial_cash = broker._cash

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    buy_response = broker.submit_order(buy_order)
    buy_price = buy_response.avg_fill_price

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=105.0,
        high=107.0,
        low=104.0,
        close=106.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    sell_response = broker.submit_order(sell_order)
    sell_price = sell_response.avg_fill_price

    expected_cash = initial_cash - (buy_price * 10.0) + (sell_price * 10.0)
    assert broker._cash == expected_cash
    assert sell_price > buy_price


def test_order_fills_at_exact_limit_price(broker, sample_candle):
    """Test that limit orders fill when price exactly matches limit"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=101.0,
        high=102.0,
        low=99.0,
        close=100.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_stop_order_fills_at_exact_stop_price(broker, sample_candle):
    """Test that stop orders fill when price exactly matches stop"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=105.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=104.0,
        high=106.0,
        low=103.0,
        close=105.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_pending_orders_not_filled_when_conditions_not_met(broker, sample_candle):
    """Test that pending orders remain pending when price conditions aren't met"""
    broker._current_candle = sample_candle

    limit_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=95.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(limit_order)

    stop_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.STOP,
        quantity=10.0,
        stop_price=110.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(stop_order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    broker.process_pending_orders()

    assert len(broker._pending_orders) == 2


def test_equity_calculation_with_unrealized_gains(broker, sample_candle):
    """Test that equity correctly reflects unrealized P&L"""
    broker._current_candle = sample_candle
    initial_equity = broker.get_account().equity

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=105.0,
        high=107.0,
        low=104.0,
        close=110.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    account = broker.get_account()
    expected_unrealized_pnl = (110.0 - 101.0) * 10.0
    expected_equity = initial_equity + expected_unrealized_pnl

    assert account.equity == expected_equity


def test_equity_calculation_with_unrealized_losses(broker, sample_candle):
    """Test that equity correctly reflects unrealized losses"""
    broker._current_candle = sample_candle
    initial_equity = broker.get_account().equity

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order)

    new_candle = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=95.0,
        high=97.0,
        low=93.0,
        close=94.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = new_candle

    account = broker.get_account()
    expected_unrealized_pnl = (94.0 - 101.0) * 10.0
    expected_equity = initial_equity + expected_unrealized_pnl

    assert account.equity == expected_equity


def test_sell_more_than_position_rejected(broker, sample_candle):
    """Test that selling more shares than owned is rejected"""
    broker._current_candle = sample_candle

    buy_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=5.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(buy_order)

    sell_order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=10.0,
        time_in_force=TimeInForce.GTC,
    )
    response = broker.submit_order(sell_order)

    assert response.status == OrderStatus.REJECTED


def test_multiple_limit_orders_fill_in_sequence(broker, sample_candle):
    """Test multiple limit orders filling as price moves"""
    broker._current_candle = sample_candle

    order1 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=100.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order1)

    order2 = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=10.0,
        limit_price=98.0,
        time_in_force=TimeInForce.GTC,
    )
    broker.submit_order(order2)

    assert len(broker._pending_orders) == 2

    candle1 = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 31),
        open=101.0,
        high=102.0,
        low=99.0,
        close=100.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = candle1
    broker.process_pending_orders()

    assert len(broker._pending_orders) == 1

    candle2 = OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 32),
        open=99.0,
        high=100.0,
        low=97.0,
        close=98.0,
        volume=1000.0,
        timeframe=Timeframe.m1,
    )
    broker._current_candle = candle2
    broker.process_pending_orders()

    assert len(broker._pending_orders) == 0


def test_account_id_is_unique(broker):
    """Test that each broker instance has a unique account ID"""
    broker2 = BacktestBroker(starting_balance=100000.0)

    assert broker._account_id != broker2._account_id


def test_cash_never_goes_negative(broker, sample_candle):
    """Test that cash balance cannot go negative"""
    broker._current_candle = sample_candle

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=1000.0,
        time_in_force=TimeInForce.GTC,
    )

    response = broker.submit_order(order)

    assert response.status == OrderStatus.REJECTED
    assert broker._cash == 100000.0
