from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from module.backtest.engine import BacktestEngine
from module.backtest.engine.ohlc_feed_client import BacktestOHLCFeedClient
from module.backtest.engine.oms_client import BacktestOMSClient
from module.broker.enums import BrokerType, OrderSide, OrderStatus, OrderType
from module.broker.schema import Order, OrderRequest
from module.event_bus import SyncEventPublisher
from module.markets.enums import MarketType, Timeframe
from module.markets.schema import OHLC as OHLCModel
from module.strategy.strategy import BaseStrategy


def _mock_db_session_for_candles(candles):
    db_sess = MagicMock()

    rows = []

    for candle in candles:
        instrument = MagicMock()
        instrument.native_symbol = candle.symbol
        instrument.broker_type = candle.broker
        instrument.market_type = candle.market_type

        ohlc = MagicMock()
        ohlc.open = candle.open
        ohlc.high = candle.high
        ohlc.low = candle.low
        ohlc.close = candle.close
        ohlc.volume = candle.volume
        ohlc.timeframe = candle.timeframe
        ohlc.timestamp = candle.timestamp

        row = MagicMock()
        row.tuple.return_value = (ohlc, instrument)

        rows.append(row)

    result = MagicMock()
    result.yield_per.return_value = rows

    db_sess.execute.return_value = result

    ctx = MagicMock()
    ctx.__enter__.return_value = db_sess
    ctx.__exit__.return_value = None

    return ctx


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
                "timeframe":[ Timeframe.m1],
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
        oms = BacktestOMSClient(starting_balance=starting_balance)

        feed = BacktestOHLCFeedClient(
            start=int(start_date.timestamp()),
            end=int(end_date.timestamp()),
        )

        feed.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "timeframe": [Timeframe.m1],
                "broker_type": BrokerType.ALPACA,
            },
        ])

        oms.ohlc_feed_client = feed

        event_pub = _make_event_publisher()
        strategy = SimpleStrategy(feed, oms, event_pub)

        engine = BacktestEngine(
            strategy,
            starting_balance,
            start_date,
            end_date,
        )

        patcher = patch(
            "module.backtest.engine.ohlc_feed_client.get_db_sess_sync",
            return_value=_mock_db_session_for_candles(candles),
        )

        patcher.start()
        self.addCleanup = patcher.stop

        return engine

    def test_returns_correct_realised_pnl(self):
        ts = lambda m: datetime(2024, 1, 1, 1, m, tzinfo=UTC)

        candles = [
            _candle(101.0, ts(1)),
            _candle(102.0, ts(2)),
            _candle(103.0, ts(3)),
            _candle(104.0, ts(4)),
        ]

        engine = self._prepare(candles, start_date=ts(1), end_date=ts(4))

        try:
            metrics = engine.run()
        finally:
            self.addCleanup()

        assert metrics.realised_pnl == 2.0
        assert metrics.unrealised_pnl == 0.0
