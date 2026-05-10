from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from enums import BrokerType, OrderSide, OrderStatus, OrderType, Timeframe
from lib.backtest_engine import BacktestEngine
from lib.brokers.backtest import BacktestBroker
from lib.strategy import BaseStrategy
from models import (
    OHLC,
    BacktestConfig,
    BacktestMetrics,
    EquityCurvePoint,
    Order,
    OrderRequest,
)


class SimpleMovingAverageStrategy(BaseStrategy):

    def __init__(self, broker, short_period: int = 5, long_period: int = 20):
        super().__init__("SMA_Strategy", broker)
        self.short_period = short_period
        self.long_period = long_period
        self._prices: list[float] = []
        self._position = 0

    def startup(self):
        self._prices = []
        self._position = 0

    def on_candle(self, candle):
        self._prices.append(candle.close)
        if len(self._prices) < self.long_period:
            return

        short_ma = sum(self._prices[-self.short_period :]) / self.short_period
        long_ma = sum(self._prices[-self.long_period :]) / self.long_period

        if short_ma > long_ma and self._position == 0:
            self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    quantity=1.0,
                )
            )
            self._position = 1
        elif short_ma < long_ma and self._position == 1:
            self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    quantity=1.0,
                )
            )
            self._position = 0

    def shutdown(self):
        pass


def create_candle(
    timestamp: int, open: float, high: float, low: float, close: float
) -> MagicMock:
    candle = MagicMock()
    candle.timestamp = timestamp
    candle.open = open
    candle.high = high
    candle.low = low
    candle.close = close
    candle.volume = 1000.0
    candle.timeframe = Timeframe.m1
    candle.symbol = "AAPL"
    return candle


def create_metrics_config() -> BacktestConfig:
    return BacktestConfig(
        timeframe=Timeframe.m1,
        starting_balance=10000.0,
        symbol="AAPL",
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        end_date=datetime(2024, 1, 31, tzinfo=UTC),
        broker=BrokerType.ALPACA,
    )


class SimpleStrategy(BaseStrategy):

    def __init__(self, name, broker):
        super().__init__(name, broker)
        self._order: Order = None

    def on_candle(self, candle):
        if self._order is None:
            self._order = self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    notional=candle.close,
                )
            )
        else:
            self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    quantity=self._order.quantity,
                )
            )
            self._order = None


class TestBacktestMetricsCalculation:

    def test_returns_correct_realised_pnl(self):
        broker = BacktestBroker(starting_balance=10000)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=101.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 1, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=101.0,
                high=106.0,
                low=96.0,
                close=102.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 2, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=102.0,
                high=107.0,
                low=97.0,
                close=103.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 3, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=103.0,
                high=108.0,
                low=98.0,
                close=104.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 4, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10_000.0,
            symbol="AAPL",
            start_date=candles[0].timestamp,
            end_date=candles[-1].timestamp,
            broker=BrokerType.ALPACA,
        )

        with patch("lib.brokers.backtest.get_db_sess_sync") as mock_get_db:
            mock_db_sess = MagicMock()

            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_db_sess
            mock_context_manager.__exit__.return_value = None

            mock_get_db.return_value = mock_context_manager

            mock_results = MagicMock()
            mock_db_sess.scalars.return_value = mock_results

            mock_results.yield_per.return_value = iter(candles)

            engine = BacktestEngine(strategy, broker, config)
            metrics = engine.run()

        assert metrics.realised_pnl == 2.0
        assert metrics.unrealised_pnl == 0.0

    def test_returns_correct_total_return_pct(self):
        broker = BacktestBroker(starting_balance=10000)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=101.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 1, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=101.0,
                high=106.0,
                low=96.0,
                close=102.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 2, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=102.0,
                high=107.0,
                low=97.0,
                close=103.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 3, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=103.0,
                high=108.0,
                low=98.0,
                close=104.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 4, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10_000.0,
            symbol="AAPL",
            start_date=candles[0].timestamp,
            end_date=candles[-1].timestamp,
            broker=BrokerType.ALPACA,
        )

        with patch("lib.brokers.backtest.get_db_sess_sync") as mock_get_db:
            mock_db_sess = MagicMock()

            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_db_sess
            mock_context_manager.__exit__.return_value = None

            mock_get_db.return_value = mock_context_manager

            mock_results = MagicMock()
            mock_db_sess.scalars.return_value = mock_results

            mock_results.yield_per.return_value = iter(candles)

            engine = BacktestEngine(strategy, broker, config)
            metrics = engine.run()

        assert metrics.total_return_pct == 0.02  # 2% return
