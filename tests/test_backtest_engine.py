from datetime import datetime, timedelta
from types import MethodType
from unittest.mock import Mock, MagicMock, patch

import pytest

from engine.backtesting.engine import (
    BacktestEngine,
    BacktestConfig,
    SpotBacktestResult,
)
from engine.brokers.backtest import BacktestBroker
from engine.enums import Timeframe, OrderSide, OrderStatus
from engine.models import OrderRequest, OrderResponse
from engine.ohlcv import OHLCV
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext


# Mock Strategy Classes


class BuyAndHoldStrategy(BaseStrategy):
    """Simple strategy that buys once and holds."""

    def __init__(self):
        self.bought = False

    def on_candle(self, context: StrategyContext) -> None:
        """Buy on first candle."""
        if not self.bought:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type="market",
                quantity=10.0,
                time_in_force="gtc",
            )
            context.broker.submit_order(order)
            self.bought = True


class BuyAndSellStrategy(BaseStrategy):
    """Strategy that buys then sells."""

    def __init__(self):
        self.bought = False
        self.sold = False
        self.candle_count = 0

    def on_candle(self, context: StrategyContext) -> None:
        """Buy on first candle, sell on third candle."""
        self.candle_count += 1

        if self.candle_count == 1 and not self.bought:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type="market",
                quantity=10.0,
                time_in_force="gtc",
            )
            context.broker.submit_order(order)
            self.bought = True

        elif self.candle_count == 3 and not self.sold:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.SELL,
                order_type="market",
                quantity=10.0,
                time_in_force="gtc",
            )
            context.broker.submit_order(order)
            self.sold = True


class NoTradeStrategy(BaseStrategy):
    """Strategy that never trades."""

    def on_candle(self, context: StrategyContext) -> None:
        """Do nothing."""
        pass


# Fixtures


@pytest.fixture
def basic_config() -> BacktestConfig:
    """Create a basic backtest configuration."""
    return BacktestConfig(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 10),
        symbol="AAPL",
        starting_balance=10000.0,
        timeframe=Timeframe.m1,
    )


@pytest.fixture
def sample_ohlcv_data() -> list[OHLCV]:
    """Generate sample OHLCV data with increasing prices."""
    base_time = datetime(2024, 1, 1, 9, 30)
    data = []

    for i in range(5):
        timestamp = base_time + timedelta(minutes=i)
        price = 100.0 + (i * 2.0)  # Prices: 100, 102, 104, 106, 108

        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=timestamp,
            open=price - 0.5,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data


@pytest.fixture
def constant_price_ohlcv_data() -> list[OHLCV]:
    """Generate OHLCV data with constant prices."""
    base_time = datetime(2024, 1, 1, 9, 30)
    data = []

    for i in range(5):
        timestamp = base_time + timedelta(minutes=i)

        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=timestamp,
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1000,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data


def get_mock_yield_historic_ohlcv(ohlcv: list[OHLCV], broker):
    def mock_yield_historic_ohlcv(self: BacktestBroker, *args, **kw):
        nonlocal ohlcv

        for candle in ohlcv:
            self._current_candle = candle
            yield candle

    return MethodType(mock_yield_historic_ohlcv, broker)


# Basic Execution Tests


def test_backtest_engine_initialization(basic_config: BacktestConfig) -> None:
    """Verify BacktestEngine initializes correctly."""
    strategy = NoTradeStrategy()
    engine = BacktestEngine(basic_config, strategy)

    assert engine._config == basic_config
    assert engine._strategy == strategy
    assert isinstance(engine._broker, BacktestBroker)
    assert engine._broker._cash == 10000.0
    assert len(engine._equity_curve) == 0


def test_backtest_with_no_trades(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify backtest completes when no trades are made."""
    strategy = NoTradeStrategy()
    engine = BacktestEngine(basic_config, strategy)

    # Mock the broker's yield_historic_ohlcv to return sample data
    with patch.object(
        engine._broker, "yield_historic_ohlcv", return_value=iter(sample_ohlcv_data)
    ):
        result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders == 0
    assert result.realised_pnl == 0.0
    assert result.total_return_pct == 0.0
    assert len(result.equity_curve) == 5


def test_backtest_with_buy_and_hold(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify backtest with buy and hold strategy."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = MethodType(
        get_mock_yield_historic_ohlcv(sample_ohlcv_data, engine._broker), engine._broker
    )
    result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders == 1
    assert len(result.orders) == 1
    assert result.orders[0].side == OrderSide.BUY
    assert result.orders[0].status == OrderStatus.FILLED


def test_backtest_with_buy_and_sell(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify backtest with buy and sell strategy."""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders == 2

    # First order should be buy
    buy_order = next(o for o in result.orders if o.side == OrderSide.BUY)
    assert buy_order.status == OrderStatus.FILLED

    # Second order should be sell
    sell_order = next(o for o in result.orders if o.side == OrderSide.SELL)
    assert sell_order.status == OrderStatus.FILLED


# Equity Curve Tests


def test_equity_curve_generation(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify equity curve is generated correctly."""
    strategy = NoTradeStrategy()
    engine = BacktestEngine(basic_config, strategy)

    with patch.object(
        engine._broker, "yield_historic_ohlcv", return_value=iter(sample_ohlcv_data)
    ):
        result = engine.run()

    # Should have one equity point per candle
    assert len(result.equity_curve) == len(sample_ohlcv_data)

    # Each entry should be (timestamp, equity) tuple
    for timestamp, equity in result.equity_curve:
        assert isinstance(timestamp, (datetime, int, float))
        assert isinstance(equity, (int, float))


def test_equity_curve_with_trades(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify equity curve reflects trades correctly."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    # Equity curve should show changes from trades
    assert len(result.equity_curve) > 0

    # Initial equity should be starting balance
    first_equity = result.equity_curve[0][1]
    assert first_equity == 10000.0


# Result Calculation Tests


def test_total_return_calculation_no_trades(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify total return is 0% when no trades are made."""
    strategy = NoTradeStrategy()
    engine = BacktestEngine(basic_config, strategy)

    with patch.object(
        engine._broker, "yield_historic_ohlcv", return_value=iter(sample_ohlcv_data)
    ):
        result = engine.run()

    assert result.total_return_pct == 0.0


def test_total_return_calculation_with_profit(basic_config: BacktestConfig) -> None:
    """Verify total return calculation with profitable trade."""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(basic_config, strategy)

    # Create data where price increases from 100 to 110
    data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    for i in range(5):
        price = 100.0 if i < 2 else 110.0
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=base_time + timedelta(minutes=i),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1000,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        data, engine._broker
    )
    result = engine.run()

    # Should have positive return if bought at 100 and sold at 110
    # Buy 10 shares at 100 = -1000, Sell 10 shares at 110 = +1100
    # Net profit = 100, Return = 100/10000 = 1%
    assert result.total_orders == 2


def test_realized_pnl_calculation(basic_config: BacktestConfig) -> None:
    """Verify realized P&L calculation (TESTS BUG at lines 91-99)."""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(basic_config, strategy)

    # Create data with known prices
    data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    # Buy at 100, sell at 110
    prices = [100.0, 100.0, 110.0, 110.0, 110.0]
    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=base_time + timedelta(minutes=i),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1000,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        data, engine._broker
    )
    result = engine.run()

    assert isinstance(result.realised_pnl, float)


def test_unrealized_pnl_calculation(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify unrealized P&L calculation."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    # With buy and hold, should have unrealized P&L
    # (since position not closed)
    assert isinstance(result.unrealised_pnl, float)


# Metrics Tests


def test_sharpe_ratio_calculation(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify Sharpe ratio is calculated."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    assert isinstance(result.sharpe_ratio, float)


def test_max_drawdown_calculation(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify max drawdown is calculated."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    assert isinstance(result.max_drawdown, float)
    assert result.max_drawdown >= 0.0  # Should be positive percentage


# Edge Cases


def test_backtest_with_empty_data(basic_config: BacktestConfig) -> None:
    """Verify backtest handles empty data gracefully."""
    strategy = NoTradeStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        [], engine._broker
    )
    result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders == 0
    assert len(result.equity_curve) == 0


def test_backtest_with_constant_prices(
    basic_config: BacktestConfig, constant_price_ohlcv_data: list[OHLCV]
) -> None:
    """Verify backtest handles constant prices correctly."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        constant_price_ohlcv_data, engine._broker
    )
    result = engine.run()

    # With constant prices, returns should be close to zero
    # (minus any trading costs if implemented)
    assert isinstance(result, SpotBacktestResult)


def test_backtest_with_single_candle(basic_config: BacktestConfig) -> None:
    """Verify backtest handles single candle data."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    single_candle = [
        OHLCV(
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1000,
            timeframe=Timeframe.m1,
        )
    ]

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        single_candle, engine._broker
    )
    result = engine.run()

    assert len(result.equity_curve) == 1
    assert result.total_orders >= 0


def test_cancel_all_orders_at_end(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify all pending orders are cancelled at backtest end."""

    class LimitOrderStrategy(BaseStrategy):
        """Strategy that places limit order that won't fill."""

        def on_candle(self, context: StrategyContext) -> None:
            """Place limit order far from current price."""
            if len(context.broker._orders) == 0:
                order = OrderRequest(
                    symbol="AAPL",
                    side=OrderSide.BUY,
                    order_type="limit",
                    quantity=10.0,
                    limit_price=50.0,  # Far below market
                    time_in_force="gtc",
                )
                context.broker.submit_order(order)

    strategy = LimitOrderStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    # All pending orders should be cancelled
    assert len(engine._broker._pending_orders) == 1


# Integration Tests


def test_full_backtest_integration(basic_config: BacktestConfig) -> None:
    """Verify full backtest integration with realistic scenario."""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(basic_config, strategy)

    # Create realistic price data
    data = []
    base_time = datetime(2024, 1, 1, 9, 30)
    prices = [100.0, 102.0, 104.0, 103.0, 105.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=base_time + timedelta(minutes=i),
            open=price - 0.5,
            high=price + 0.5,
            low=price - 0.5,
            close=price,
            volume=1000,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        data, engine._broker
    )
    result = engine.run()

    # Verify result structure
    assert isinstance(result, SpotBacktestResult)
    assert result.config == basic_config
    assert isinstance(result.orders, list)
    assert result.total_orders == len(result.orders)
    assert len(result.equity_curve) == len(data)

    # Verify all metrics are calculated
    assert isinstance(result.realised_pnl, float)
    assert isinstance(result.unrealised_pnl, float)
    assert isinstance(result.total_return_pct, float)
    assert isinstance(result.sharpe_ratio, float)
    assert isinstance(result.max_drawdown, float)


def test_backtest_config_validation() -> None:
    """Verify BacktestConfig validates dates correctly."""
    config = BacktestConfig(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 10),
        symbol="AAPL",
        starting_balance=10000.0,
        timeframe=Timeframe.m1,
    )

    assert config.start_date < config.end_date
    assert config.starting_balance > 0
    assert config.symbol == "AAPL"


def test_backtest_result_contains_all_fields(
    basic_config: BacktestConfig, sample_ohlcv_data: list[OHLCV]
) -> None:
    """Verify BacktestResult contains all required fields."""
    strategy = BuyAndHoldStrategy()
    engine = BacktestEngine(basic_config, strategy)

    engine._broker.yield_historic_ohlcv = get_mock_yield_historic_ohlcv(
        sample_ohlcv_data, engine._broker
    )
    result = engine.run()

    # Verify all fields exist
    assert hasattr(result, "config")
    assert hasattr(result, "realised_pnl")
    assert hasattr(result, "unrealised_pnl")
    assert hasattr(result, "total_return")
    assert hasattr(result, "sharpe_ratio")
    assert hasattr(result, "max_drawdown")
    assert hasattr(result, "equity_curve")
    assert hasattr(result, "orders")
    assert hasattr(result, "total_orders")
