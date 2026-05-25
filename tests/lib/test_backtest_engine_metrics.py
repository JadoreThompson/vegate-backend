from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from module.backtest.oms_client import BacktestOMSClient
from module.backtest.engine import BacktestEngine
from module.broker.enums import BrokerType, OrderSide, OrderStatus, OrderType
from module.broker.schema import Order, OrderRequest
from module.event_bus import SyncEventPublisher
from module.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC
from module.markets.schema import OHLC as OHLCModel
from module.strategy.strategy import BaseStrategy
from core.db import get_db_sess_sync


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


def _make_ohlc_feed_client(
    candles: list, market_type=MarketType.STOCKS, timeframe=Timeframe.m1
):
    """Build a mocked OHLCFeedClient that yields the given candles."""
    feed = MagicMock()
    feed.timeframe = timeframe
    feed.start = candles[0].timestamp if candles else 0
    feed.market_type = market_type

    def _generator():
        for c in candles:
            feed.cur_candle = c
            yield c

    feed.candles.side_effect = lambda: _generator()
    feed.cur_candle = candles[0] if candles else MagicMock()
    return feed


def _make_event_publisher():
    return MagicMock(spec=SyncEventPublisher)


class SimpleStrategy(BaseStrategy):

    def __init__(self, ohlc_feed_client, oms_client, event_publisher):
        super().__init__(ohlc_feed_client, oms_client, event_publisher)
        self._order: Order = None
        self._quantity = 1

    def startup(self):
        self.ohlc_feed_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "timeframe": Timeframe.m1,
                "broker_type": BrokerType.ALPACA,
            },
        ])

    def on_candle(self, candle):
        if self._order is None:
            self._order = self.oms_client.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    quantity=self._quantity,
                ),
                candle.timestamp,
            )
        else:
            self.oms_client.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    quantity=self._quantity,
                ),
                candle.timestamp,
            )
            self._order = None


def _candle(close, ts, **kw):
    defaults = dict(
        open=close,
        high=close + 5.0,
        low=close - 5.0,
        volume=0.0,
        timeframe=Timeframe.m1,
        symbol="AAPL",
        broker=BrokerType.ALPACA,
        market_type=MarketType.STOCKS,
    )
    defaults.update(kw)
    return OHLCModel(close=close, timestamp=int(ts.timestamp()), **defaults)


class TestBacktestMetricsCalculation:

    def _prepare(
        self,
        candles,
        starting_balance=10_000.0,
        start_date=datetime(year=2024, month=1, day=1),
        end_date=datetime(year=2025, month=1, day=1),
    ):
        market_type = MarketType.STOCKS
        oms = BacktestOMSClient(starting_balance=starting_balance)
        feed = _make_ohlc_feed_client(candles, market_type=market_type)
        event_pub = _make_event_publisher()
        strategy = SimpleStrategy(feed, oms, event_pub)
        engine = BacktestEngine(strategy, starting_balance, start_date, end_date)
        return engine

    def test_returns_correct_realised_pnl(self):
        ts = lambda m: datetime(2024, 1, 1, 1, m, tzinfo=UTC)
        candles = [
            _candle(101.0, ts(1)),
            _candle(102.0, ts(2)),
            _candle(103.0, ts(3)),
            _candle(104.0, ts(4)),
        ]
        engine = self._prepare(candles)
        metrics = engine.run()
        assert metrics.realised_pnl == 2.0
        assert metrics.unrealised_pnl == 0.0

    def test_returns_correct_total_return_pct(self):
        ts = lambda m: datetime(2024, 1, 1, 1, m, tzinfo=UTC)
        candles = [
            _candle(101.0, ts(1)),
            _candle(102.0, ts(2)),
            _candle(103.0, ts(3)),
            _candle(104.0, ts(4)),
        ]
        engine = self._prepare(candles)
        metrics = engine.run()
        assert metrics.total_return_pct == 0.02

    def test_profit_factor_with_winning_trade(self):
        ts = lambda i: datetime(2024, 1, 1, 1, i + 1, tzinfo=UTC)
        candles = [_candle(100.0 + i, ts(i)) for i in range(9)]
        engine = self._prepare(candles)
        metrics = engine.run()
        assert metrics.profit_factor == float("inf")

    def test_profit_factor_with_mixed_trades(self):
        ts = lambda m: datetime(2024, 1, 1, 1, m, tzinfo=UTC)
        candles = [
            _candle(100.0, ts(1)),
            _candle(102.0, ts(2)),
            _candle(102.0, ts(3)),
            _candle(101.0, ts(4)),
        ]
        engine = self._prepare(candles)
        metrics = engine.run()
        assert metrics.profit_factor == 2.0

    def test_profit_factor_with_losing_trade(self):
        ts = lambda i: datetime(2024, 1, 1, 1, i, tzinfo=UTC)
        candles = [_candle(100.0 - i * 0.5, ts(i)) for i in range(1, 9)]
        engine = self._prepare(candles)
        metrics = engine.run()
        assert metrics.profit_factor == 0.0

    def test_profit_factor_zero_with_no_trades(self):
        class NoTradeStrategy(BaseStrategy):
            def __init__(self, ohlc_feed_client, oms_client, event_publisher):
                super().__init__(ohlc_feed_client, oms_client, event_publisher)

            def on_candle(self, candle):
                pass

        market_type = MarketType.STOCKS
        ts = lambda i: datetime(2024, 1, 1, 1, i, tzinfo=UTC)
        candles = [_candle(100.0 + i * 0.5, ts(i)) for i in range(1, 21)]
        oms = BacktestOMSClient(starting_balance=10000)
        feed = _make_ohlc_feed_client(candles, market_type=market_type)
        event_pub = _make_event_publisher()
        strategy = NoTradeStrategy(feed, oms, event_pub)
        engine = BacktestEngine(
            strategy,
            10000,
            start_date=datetime(year=2024, month=1, day=1),
            end_date=datetime(year=2025, month=1, day=1),
        )
        metrics = engine.run()
        assert metrics.profit_factor == 0.0

    def test_profit_factor_with_partially_filled_orders(self):
        class PartialFillStrategy(BaseStrategy):
            def __init__(self, ohlc_feed_client, oms_client, event_publisher):
                super().__init__(ohlc_feed_client, oms_client, event_publisher)
                self._order: Order = None

            def on_candle(self, candle):
                if self._order is None:
                    self._order = self.oms_client.place_order(
                        OrderRequest(
                            symbol=candle.symbol,
                            order_type=OrderType.MARKET,
                            side=OrderSide.BUY,
                            quantity=2.0,
                        ),
                        candle.timestamp,
                    )
                    self._order.status = OrderStatus.PARTIALLY_FILLED
                    self._order.filled_quantity = 1.0
                else:
                    self.oms_client.place_order(
                        OrderRequest(
                            symbol=candle.symbol,
                            order_type=OrderType.MARKET,
                            side=OrderSide.SELL,
                            quantity=1.0,
                        ),
                        candle.timestamp,
                    )
                    self._order = None

        ts = lambda m: datetime(2024, 1, 1, 1, m, tzinfo=UTC)
        candles = [_candle(100.0, ts(1)), _candle(110.0, ts(2))]
        market_type = MarketType.STOCKS
        oms = BacktestOMSClient(starting_balance=10000)
        feed = _make_ohlc_feed_client(candles, market_type=market_type)
        event_pub = _make_event_publisher()
        strategy = PartialFillStrategy(feed, oms, event_pub)
        engine = BacktestEngine(
            strategy,
            10000,
            start_date=datetime(year=2024, month=1, day=1),
            end_date=datetime(year=2025, month=1, day=1),
        )
        metrics = engine.run()
        assert metrics.profit_factor == float("inf")


class TestBacktestEngineIntegration:
    """Integration tests for BacktestEngine using a real database session."""

    @pytest.fixture()
    def seed_and_teardown(self):
        """Seed candles into the DB before each test and clean up after."""
        symbol = "AAPL"
        broker = BrokerType.ALPACA
        timeframe = Timeframe.m1

        self._candle_data = [
            {
                "open": 100.0,
                "high": 105.0,
                "low": 95.0,
                "close": 100.0,
                "volume": 1000.0,
                "timestamp": int(datetime(2024, 1, 2, 0, 1, tzinfo=UTC).timestamp()),
                "timeframe": timeframe,
                "instrument_id": None,
            },
            {
                "open": 110.0,
                "high": 115.0,
                "low": 105.0,
                "close": 110.0,
                "volume": 1000.0,
                "timestamp": int(datetime(2024, 1, 2, 0, 2, tzinfo=UTC).timestamp()),
                "timeframe": timeframe,
                "instrument_id": None,
            },
        ]

        with get_db_sess_sync() as db_session:
            from sqlalchemy import insert, delete
            from module.markets.model import Instrument

            db_session.execute(
                delete(Instrument).where(
                    Instrument.symbol == symbol,
                    Instrument.native_symbol == symbol,
                    Instrument.broker_type == broker,
                    Instrument.market_type == MarketType.STOCKS,
                )
            )

            instr = db_session.execute(
                insert(Instrument)
                .values(
                    symbol=symbol,
                    native_symbol=symbol,
                    broker_type=broker,
                    market_type=MarketType.STOCKS,
                )
                .returning(Instrument)
            ).scalar()

            for d in self._candle_data:
                d["instrument_id"] = instr.id
            rows = [OHLC(**d) for d in self._candle_data]
            db_session.add_all(rows)
            db_session.commit()

        yield

        with get_db_sess_sync() as db_session:
            db_session.query(OHLC).delete()
            db_session.query(Instrument).delete()
            db_session.commit()

    def test_backtest_metrics_all_fields(self, seed_and_teardown):
        starting_balance = 10_000.0
        symbol = "AAPL"
        market_type = MarketType.STOCKS

        oms = BacktestOMSClient(starting_balance=starting_balance)
        from module.backtest.ohlc_feed_client import BacktestOHLCFeedClient

        feed = BacktestOHLCFeedClient(
            start=int(datetime(2024, 1, 2, tzinfo=UTC).timestamp()),
            end=int(datetime(2024, 1, 3, 0, 0, 0, tzinfo=UTC).timestamp()),
        )

        feed.subscribe([
            {
                "symbol": symbol,
                "market_type": market_type,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])
        
        event_pub = _make_event_publisher()
        strategy = SimpleStrategy(feed, oms, event_pub)
        engine = BacktestEngine(
            strategy,
            10000,
            start_date=datetime(year=2024, month=1, day=1),
            end_date=datetime(year=2024, month=1, day=1),
        )
        metrics = engine.run()

        assert metrics.realised_pnl == 10.0
        assert metrics.unrealised_pnl == 0.0
        assert metrics.total_return_pct == 0.1
        assert metrics.total_orders == 2
        assert metrics.profit_factor == float("inf")

        orders = oms.get_orders()
        assert len(orders) == 2
        filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]
        assert len(filled_orders) == 2

        buy_order = next(o for o in filled_orders if o.side == OrderSide.BUY)
        sell_order = next(o for o in filled_orders if o.side == OrderSide.SELL)
        assert buy_order.symbol == symbol
        assert buy_order.avg_fill_price == 100.0
        assert buy_order.filled_quantity == 1.0
        assert sell_order.symbol == symbol
        assert sell_order.avg_fill_price == 110.0
        assert sell_order.filled_quantity == 1.0

        assert len(metrics.equity_curve) == 3
        assert metrics.equity_curve[0].balance == starting_balance
        assert metrics.equity_curve[-1].balance == starting_balance + 10.0
