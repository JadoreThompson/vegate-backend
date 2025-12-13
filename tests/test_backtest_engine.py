import pytest
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock

from engine.backtesting.engine import BacktestEngine, BacktestConfig, SpotBacktestResult
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext
from engine.enums import Timeframe
from engine.models import OrderRequest, OrderSide, OrderType, TimeInForce
from engine.ohlcv import OHLCV
from engine.brokers.backtest import BacktestBroker


class SimpleStrategy(BaseStrategy):
    """Simple buy and hold strategy for testing"""

    def __init__(self):
        self.executed = False

    def on_candle(self, context: StrategyContext):
        if not self.executed:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10.0,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.executed = True


class BuyAndSellStrategy(BaseStrategy):
    """Strategy that buys and then sells for testing"""

    def __init__(self):
        self.buy_executed = False
        self.sell_executed = False
        self.candle_count = 0

    def on_candle(self, context: StrategyContext):
        self.candle_count += 1

        if not self.buy_executed and self.candle_count == 1:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10.0,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.buy_executed = True

        elif not self.sell_executed and self.candle_count == 5:
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=10.0,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.sell_executed = True


class NeverTradeStrategy(BaseStrategy):
    """Strategy that never places orders"""

    def on_candle(self, context: StrategyContext):
        pass


@pytest.fixture
def backtest_config():
    return BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 10),
        symbol="AAPL",
        starting_balance=100000.0,
        timeframe=Timeframe.m1,
    )


@pytest.fixture
def mock_ohlcv_data():
    """Create mock OHLCV data for testing"""
    data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.0, 101.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price - 0.5,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data


def mock_yield_historic_ohlcv(broker, candles):
    for candle in candles:
        broker._current_candle = candle
        yield candle


def test_backtest_config_initialization():
    config = BacktestConfig(
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 10),
        symbol="AAPL",
        starting_balance=100000.0,
        timeframe=Timeframe.m1,
    )

    assert config.start_date == date(2024, 1, 1)
    assert config.end_date == date(2024, 1, 10)
    assert config.symbol == "AAPL"
    assert config.starting_balance == 100000.0
    assert config.timeframe == Timeframe.m1


def test_backtest_engine_initialization(backtest_config):
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    assert engine._config == backtest_config
    assert engine._strategy == strategy
    assert isinstance(engine._broker, BacktestBroker)
    assert engine._equity_curve == []
    assert engine._cash_balance_curve == []


def test_backtest_engine_with_simple_strategy(backtest_config, mock_ohlcv_data):
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.config == backtest_config
    assert result.total_orders > 0
    assert len(result.orders) > 0
    assert len(result.equity_curve) > 0


def test_backtest_engine_with_buy_and_sell_strategy(backtest_config, mock_ohlcv_data):
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders >= 2
    assert len(result.orders) >= 2


def test_backtest_engine_with_no_trades(backtest_config, mock_ohlcv_data):
    strategy = NeverTradeStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert isinstance(result, SpotBacktestResult)
    assert result.total_orders == 0
    assert len(result.orders) == 0
    assert result.realised_pnl == 0.0


def test_backtest_result_equity_curve_tracking(backtest_config, mock_ohlcv_data):
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert len(result.equity_curve) == len(mock_ohlcv_data)

    for timestamp, equity in result.equity_curve:
        assert isinstance(timestamp, datetime)
        assert isinstance(equity, (int, float))
        assert equity > 0


def test_backtest_result_metrics_calculated(backtest_config, mock_ohlcv_data):
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert isinstance(result.realised_pnl, float)
    assert isinstance(result.unrealised_pnl, float)
    assert isinstance(result.total_return_pct, float)
    assert isinstance(result.sharpe_ratio, float)
    assert isinstance(result.max_drawdown, float)


def test_backtest_result_values_rounded(backtest_config, mock_ohlcv_data):
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    assert result.realised_pnl == round(result.realised_pnl, 2)
    assert result.unrealised_pnl == round(result.unrealised_pnl, 2)
    assert result.total_return_pct == round(result.total_return_pct, 2)
    assert result.sharpe_ratio == round(result.sharpe_ratio, 2)
    assert result.max_drawdown == round(result.max_drawdown, 2)


def test_backtest_engine_profitable_scenario(backtest_config):
    """Test a scenario where strategy makes profit"""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    mock_data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        mock_data.append(ohlcv)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_data),
    ):
        result = engine.run()

    assert result.realised_pnl > 0
    assert result.total_return_pct > 0


def test_backtest_engine_losing_scenario(backtest_config):
    """Test a scenario where strategy loses money"""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    mock_data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0, 94.0, 93.0, 92.0, 91.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        mock_data.append(ohlcv)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_data),
    ):
        result = engine.run()

    assert result.realised_pnl < 0
    assert result.total_return_pct < 0


def test_backtest_engine_empty_data(backtest_config):
    """Test engine behavior with no data"""
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(engine._broker, "yield_historic_ohlcv", return_value=iter([])):
        result = engine.run()

    assert result.total_orders == 0
    assert len(result.equity_curve) == 0


def test_backtest_engine_single_candle(backtest_config):
    """Test engine with single candle"""
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    single_candle = [
        OHLCV(
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=102.0,
            low=98.0,
            close=101.0,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
    ]

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, single_candle),
    ):
        result = engine.run()

    assert len(result.equity_curve) == 1
    assert result.sharpe_ratio == 0.0


def test_backtest_engine_max_drawdown_calculation(backtest_config):
    """Test max drawdown is correctly calculated"""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    mock_data = []
    base_time = datetime(2024, 1, 1, 9, 30)

    prices = [100.0, 110.0, 105.0, 95.0, 90.0, 100.0, 105.0, 110.0, 115.0, 120.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price,
            high=price + 2.0,
            low=price - 2.0,
            close=price,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        mock_data.append(ohlcv)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_data),
    ):
        result = engine.run()

    assert result.max_drawdown < 0


def test_backtest_engine_account_balance_tracking(backtest_config, mock_ohlcv_data):
    """Test that account balance is tracked correctly"""
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()
        account = engine._broker.get_account()

    assert account.cash <= backtest_config.starting_balance
    assert account.equity > 0


def test_backtest_engine_order_details(backtest_config, mock_ohlcv_data):
    """Test that order details are captured correctly"""
    strategy = BuyAndSellStrategy()
    engine = BacktestEngine(strategy, backtest_config)

    with patch.object(
        engine._broker,
        "yield_historic_ohlcv",
        return_value=mock_yield_historic_ohlcv(engine._broker, mock_ohlcv_data),
    ):
        result = engine.run()

    for order in result.orders:
        assert order.symbol == "AAPL"
        assert order.side in [OrderSide.BUY, OrderSide.SELL]
        assert order.order_type == OrderType.MARKET
        assert order.quantity > 0
