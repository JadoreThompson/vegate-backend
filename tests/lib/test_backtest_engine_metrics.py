from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from enums import BrokerType, OrderSide, OrderStatus, OrderType, Timeframe
from infra.db.model.ohlcs import OHLCs
from infra.db.utils import get_db_sess_sync
from lib.backtest_engine import BacktestEngine
from lib.brokers.backtest import BacktestBroker
from lib.strategy import BaseStrategy
from models import (
    OHLC,
    BacktestConfig,
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

    def test_profit_factor_with_winning_trade(self):
        broker = BacktestBroker(starting_balance=10000)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.0 + i,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, i + 1, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            )
            for i in range(9)
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10000.0,
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

        assert metrics.profit_factor == float(
            "inf"
        ), f"Expected profit factor to be infinity when gross loss is 0, got {metrics.profit_factor}"

    def test_profit_factor_with_mixed_trades(self):
        broker = BacktestBroker(starting_balance=10000)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 1, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=102.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 2, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=102.0,
                high=105.0,
                low=95.0,
                close=102.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 3, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=102.0,
                high=105.0,
                low=95.0,
                close=101.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 3, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10000.0,
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

        assert (
            metrics.profit_factor == 2.0
        ), f"Expected profit factor of 2.0 for 2 profit and 1 loss, got {metrics.profit_factor}"

    def test_profit_factor_with_losing_trade(self):
        broker = BacktestBroker(starting_balance=10000)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        candles = [
            OHLC(
                open=100.0 - i * 0.5,
                high=105.0 - i * 0.5,
                low=95.0 - i * 0.5,
                close=100.0 - i * 0.5,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, i, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            )
            for i in range(1, 9)
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10000.0,
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

        assert (
            metrics.profit_factor == 0.0
        ), f"Expected profit factor of 0.0 for only losing trades, got {metrics.profit_factor}"

    def test_profit_factor_zero_with_no_trades(self):
        broker = BacktestBroker(starting_balance=10000)

        class NoTradeStrategy(BaseStrategy):
            def __init__(self, broker):
                super().__init__("NoTrade", broker)

            def on_candle(self, candle):
                pass

        strategy = NoTradeStrategy(broker)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.0 + i * 0.5,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, i, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            )
            for i in range(1, 21)
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10000.0,
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

        assert (
            metrics.profit_factor == 0.0
        ), f"Expected profit factor 0.0 with no trades, got {metrics.profit_factor}"

    def test_profit_factor_with_partially_filled_orders(self):
        broker = BacktestBroker(starting_balance=10000)

        class PartialFillStrategy(BaseStrategy):
            def __init__(self, broker):
                super().__init__("PartialFillStrategy", broker)
                self._order: Order = None

            def on_candle(self, candle):
                if self._order is None:
                    self._order = self.broker.place_order(
                        OrderRequest(
                            symbol=candle.symbol,
                            order_type=OrderType.MARKET,
                            side=OrderSide.BUY,
                            quantity=2.0,
                        )
                    )
                    # Simulate partial fill: half the quantity at half the cost
                    self._order.status = OrderStatus.PARTIALLY_FILLED
                    self._order.executed_quantity = 1.0
                else:
                    sell_order = self.broker.place_order(
                        OrderRequest(
                            symbol=candle.symbol,
                            order_type=OrderType.MARKET,
                            side=OrderSide.SELL,
                            quantity=1.0,
                        )
                    )
                    self._order = None

        strategy = PartialFillStrategy(broker)

        # Buy at 100, sell at 110 — expect profit of 10.0 on 1 unit, no losses
        # profit_factor = inf (gross_profit=10, gross_loss=0)
        candles = [
            OHLC(
                open=100.0,
                high=105.0,
                low=95.0,
                close=100.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 1, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
            OHLC(
                open=110.0,
                high=115.0,
                low=105.0,
                close=110.0,
                volume=0.0,
                timestamp=datetime(2024, 1, 1, 1, 2, tzinfo=UTC),
                timeframe=Timeframe.m1,
                symbol="AAPL",
            ),
        ]

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=10000.0,
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

        # Partially filled buy: 1 unit @ 100. Sell: 1 unit @ 110. PnL = 10.0, no losses.
        assert metrics.profit_factor == float("inf"), (
            f"Expected profit factor of inf for partially filled buy then winning sell, "
            f"got {metrics.profit_factor}"
        )


class TestBacktestEngineIntegration:
    """Integration tests for BacktestEngine using a real database session."""

    @pytest.fixture()
    def seed_and_teardown(self):
        """Seed candles into the DB before each test and clean up after."""
        symbol = "AAPL"
        source = BrokerType.ALPACA
        timeframe = Timeframe.m1

        # Candle timestamps as unix integers within the config date range
        # Buy at candle 0 (close=100), sell at candle 1 (close=110)
        # => PnL = 10.0, no losses => profit_factor = inf
        self._candle_data = [
            {
                "source": source,
                "symbol": symbol,
                "timeframe": timeframe,
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 100.0,
                "timestamp": int(datetime(2024, 1, 2, 0, 1, tzinfo=UTC).timestamp()),
            },
            {
                "source": source,
                "symbol": symbol,
                "timeframe": timeframe,
                "open": 110.0,
                "high": 115.0,
                "low": 105.0,
                "close": 110.0,
                "timestamp": int(datetime(2024, 1, 2, 0, 2, tzinfo=UTC).timestamp()),
            },
        ]

        with get_db_sess_sync() as db_session:
            rows = [OHLCs(**data) for data in self._candle_data]
            db_session.add_all(rows)
            db_session.commit()

        yield

        with get_db_sess_sync() as db_session:
            db_session.query(OHLCs).filter(
                OHLCs.symbol == symbol,
                OHLCs.source == source,
            ).delete()
            db_session.commit()

    def test_backtest_metrics_all_fields(self, seed_and_teardown):
        """
        Full integration test: seeds DB, runs backtest, asserts every
        BacktestMetrics field is correct.

        Trade sequence:
          Candle 0 (close=100): SimpleStrategy buys 1 unit notional=100 @ 100
          Candle 1 (close=110): SimpleStrategy sells 1 unit @ 110
          PnL = 110 - 100 = 10.0
          starting_balance = 10_000
          end_balance      = 10_010
          realised_pnl     = 10.0
          unrealised_pnl   = 0.0  (position closed)
          total_return_pct = (10 / 10_000) * 100 = 0.1
          total_orders     = 2 (1 buy, 1 sell)
          profit_factor    = inf (gross_profit=10, gross_loss=0)
        """
        starting_balance = 10_000.0
        symbol = "AAPL"

        config = BacktestConfig(
            timeframe=Timeframe.m1,
            starting_balance=starting_balance,
            symbol=symbol,
            start_date=datetime(2024, 1, 2, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
            broker=BrokerType.ALPACA,
        )

        broker = BacktestBroker(starting_balance=starting_balance)
        strategy = SimpleStrategy("SimpleStrategy", broker)
        engine = BacktestEngine(strategy, broker, config)

        metrics = engine.run()

        # --- config ---
        assert metrics.config.symbol == symbol
        assert metrics.config.timeframe == Timeframe.m1
        assert metrics.config.broker == BrokerType.ALPACA
        assert metrics.config.starting_balance == starting_balance

        # --- financial metrics ---
        assert metrics.realised_pnl == 10.0
        assert metrics.unrealised_pnl == 0.0
        assert metrics.total_return_pct == 0.1  # 10/10000 * 100, rounded to 2dp

        # --- orders ---
        assert metrics.total_orders == 2
        assert len(metrics.orders) == 2

        filled_orders = [o for o in metrics.orders if o.status == OrderStatus.FILLED]
        assert len(filled_orders) == 2

        buy_order = next(o for o in filled_orders if o.side == OrderSide.BUY)
        sell_order = next(o for o in filled_orders if o.side == OrderSide.SELL)

        assert buy_order.symbol == symbol
        assert buy_order.filled_avg_price == 100.0
        assert buy_order.executed_quantity == 1.0

        assert sell_order.symbol == symbol
        assert sell_order.filled_avg_price == 110.0
        assert sell_order.executed_quantity == 1.0

        # --- profit factor ---
        assert metrics.profit_factor == float("inf")

        # --- equity curve ---
        # Initial point + one point per candle = 3 total
        assert len(metrics.equity_curve) == 3
        assert metrics.equity_curve[0].value == starting_balance
        assert metrics.equity_curve[-1].value == starting_balance + 10.0
