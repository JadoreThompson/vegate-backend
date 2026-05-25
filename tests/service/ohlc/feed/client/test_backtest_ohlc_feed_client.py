import pytest
import pytest_asyncio
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock
from uuid import uuid4

from sqlalchemy import delete, select, insert

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC, Instrument
from core.db import get_db_sess_sync, get_db_session
from module.markets.schema import OHLC as OHLCModel
from module.backtest.ohlc_feed_client import BacktestOHLCFeedClient


@pytest.fixture
def backtest_client():
    return BacktestOHLCFeedClient(start=1000, end=2000)


class TestInit:
    """Unit tests for BacktestOHLCFeedClient initialization."""

    def test_init_sets_start_and_end(self):
        client = BacktestOHLCFeedClient(start=1000, end=2000)
        assert client._start == 1000
        assert client._end == 2000

    def test_init_default_values(self, backtest_client):
        assert backtest_client._subscriptions == []
        assert backtest_client._timeframe is None
        assert backtest_client._cur_candle is None

    def test_init_name(self, backtest_client):
        assert backtest_client._name == "BacktestOHLCFeedClient"

    def test_init_logger(self, backtest_client):
        assert backtest_client._logger is not None
        assert backtest_client._logger.name == "BacktestOHLCFeedClient"


class TestProperties:
    """Unit tests for BacktestOHLCFeedClient properties."""

    def test_timeframe_property(self, backtest_client):
        backtest_client._timeframe = Timeframe.H1
        assert backtest_client.timeframe == Timeframe.H1

    def test_timeframe_property_none(self, backtest_client):
        assert backtest_client.timeframe is None

    def test_cur_candle_property(self, backtest_client):
        mock_candle = MagicMock(spec=OHLCModel)
        backtest_client._cur_candle = mock_candle
        assert backtest_client.cur_candle == mock_candle

    def test_cur_candle_property_none(self, backtest_client):
        assert backtest_client.cur_candle is None


class TestSubscribe:
    """Unit tests for the subscribe method."""

    def test_subscribe_stores_subscription(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])
        assert len(backtest_client._subscriptions) == 1
        sub = backtest_client._subscriptions[0]
        assert sub["symbol"] == "AAPL"
        assert sub["market_type"] == "stocks"
        assert sub["broker_type"] == "alpaca"
        assert sub["timeframe"] == "1m"

    def test_subscribe_multiple_timeframes(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": [Timeframe.m1, Timeframe.m5],
            },
        ])
        assert len(backtest_client._subscriptions) == 2
        assert backtest_client._subscriptions[0]["timeframe"] == "1m"
        assert backtest_client._subscriptions[1]["timeframe"] == "5m"

    def test_subscribe_multiple_instruments(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
            {
                "symbol": "BTC/USD",
                "market_type": MarketType.CRYPTO,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.H1,
            },
        ])
        assert len(backtest_client._subscriptions) == 2
        assert backtest_client._subscriptions[0]["symbol"] == "AAPL"
        assert backtest_client._subscriptions[1]["symbol"] == "BTC/USD"

    def test_subscribe_sets_timeframe_from_first(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.H1,
            },
        ])
        assert backtest_client._timeframe == Timeframe.H1
        assert backtest_client.timeframe == Timeframe.H1

    def test_subscribe_empty_instruments(self, backtest_client):
        backtest_client.subscribe([])
        assert backtest_client._subscriptions == []
        assert backtest_client._timeframe is None

    def test_subscribe_sets_name(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])
        assert "BacktestOHLCFeedClient" in backtest_client._name
        assert "AAPL" in backtest_client._name

    def test_subscribe_crypto_symbol(self, backtest_client):
        backtest_client.subscribe([
            {
                "symbol": "BTC/USD",
                "market_type": MarketType.CRYPTO,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.H1,
            },
        ])
        assert len(backtest_client._subscriptions) == 1
        assert backtest_client._subscriptions[0]["symbol"] == "BTC/USD"
        assert backtest_client._subscriptions[0]["market_type"] == "crypto"


class TestCandles:
    """Unit tests for the candles generator method."""

    def test_candles_yields_ohlc_models(self, backtest_client):
        mock_instrument = MagicMock()
        mock_instrument.symbol = "AAPL"
        mock_instrument.broker_type = BrokerType.ALPACA
        mock_instrument.market_type = MarketType.STOCKS

        mock_candle = MagicMock()
        mock_candle.open = 100.0
        mock_candle.high = 105.0
        mock_candle.low = 99.0
        mock_candle.close = 102.0
        mock_candle.volume = 1000.0
        mock_candle.timeframe = Timeframe.m1
        mock_candle.timestamp = 1500

        mock_row = MagicMock()
        mock_row.tuple.return_value = (mock_candle, mock_instrument)

        mock_result = MagicMock()
        mock_result.yield_per.return_value = [mock_row]

        mock_db_sess = MagicMock()
        mock_db_sess.execute.return_value = mock_result

        backtest_client.subscribe([
            {
                "symbol": "AAPL",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])

        with patch("module.backtest.ohlc_feed_client.get_db_sess_sync") as mock_get_db:
            mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
            mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

            candles = list(backtest_client.candles())

        assert len(candles) == 1
        assert isinstance(candles[0], OHLCModel)
        assert candles[0].open == 100.0
        assert candles[0].high == 105.0
        assert candles[0].low == 99.0
        assert candles[0].close == 102.0
        assert candles[0].volume == 1000.0
        assert candles[0].symbol == "AAPL"
        assert candles[0].broker == BrokerType.ALPACA
        assert candles[0].market_type == MarketType.STOCKS
        assert candles[0].timeframe == Timeframe.m1
        assert candles[0].timestamp == 1500


class TestIntegration:
    """Integration tests for BacktestOHLCFeedClient with real database."""

    def _create_instrument_and_candles(
        self, symbol, market_type=MarketType.STOCKS, broker_type=BrokerType.ALPACA,
        timestamps=None, start_price=100.0,
    ):
        if timestamps is None:
            timestamps = [1500]
        with get_db_sess_sync() as db_sess:
            instrument = Instrument(
                symbol=symbol,
                native_symbol=symbol,
                market_type=market_type,
                broker_type=broker_type,
            )
            db_sess.add(instrument)
            db_sess.flush()
            db_sess.refresh(instrument)
            for i, ts in enumerate(timestamps):
                ohlc = OHLC(
                    instrument_id=instrument.id,
                    open=start_price + i,
                    high=start_price + 5 + i,
                    low=start_price - 1 + i,
                    close=start_price + 2 + i,
                    volume=1000.0 + i,
                    timeframe=Timeframe.m1,
                    timestamp=ts,
                )
                db_sess.add(ohlc)
            db_sess.commit()
            inst_id = instrument.id
        return inst_id

    def _cleanup(self, instrument_ids):
        with get_db_sess_sync() as db_sess:
            for inst_id in instrument_ids:
                db_sess.execute(delete(OHLC).where(OHLC.instrument_id == inst_id))
                db_sess.execute(delete(Instrument).where(Instrument.id == inst_id))
            db_sess.commit()

    def test_integration_persist_and_iterate_candles(self, backtest_client):
        """Test end-to-end with real database records."""
        inst_id = self._create_instrument_and_candles(
            "INTTEST", timestamps=[1500 + i for i in range(5)],
        )

        backtest_client.subscribe([
            {
                "symbol": "INTTEST",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])

        candles = list(backtest_client.candles())

        assert len(candles) == 5
        for i, candle in enumerate(candles):
            assert isinstance(candle, OHLCModel)
            assert candle.open == 100.0 + i
            assert candle.close == 102.0 + i
            assert candle.timestamp == 1500 + i
            assert candle.symbol == "INTTEST"
            assert candle.broker == BrokerType.ALPACA
            assert candle.market_type == MarketType.STOCKS

        self._cleanup([inst_id])

    def test_integration_respects_start_end_range(self, backtest_client):
        """Test that start/end timestamps filter correctly."""
        inst_id = self._create_instrument_and_candles(
            "RANGETEST",
            timestamps=[1000, 1200, 1400, 1600, 1800, 2000],
        )

        backtest_client.subscribe([
            {
                "symbol": "RANGETEST",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])
        backtest_client._end = 1800

        candles = list(backtest_client.candles())

        assert len(candles) == 5
        assert candles[2].timestamp == 1400
        assert candles[3].timestamp == 1600
        assert candles[4].timestamp == 1800

        self._cleanup([inst_id])

    def test_integration_cur_candle_updated(self, backtest_client):
        """Test that cur_candle is updated during iteration."""
        inst_id = self._create_instrument_and_candles(
            "CURTEST", timestamps=[1500 + i for i in range(3)],
        )

        backtest_client.subscribe([
            {
                "symbol": "CURTEST",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])

        gen = backtest_client.candles()

        first = next(gen)
        assert backtest_client.cur_candle == first
        assert backtest_client.cur_candle.open == 100.0

        second = next(gen)
        assert backtest_client.cur_candle == second
        assert backtest_client.cur_candle.open == 101.0

        third = next(gen)
        assert backtest_client.cur_candle == third
        assert backtest_client.cur_candle.open == 102.0

        self._cleanup([inst_id])

    def test_integration_multi_instrument_ordered(self, backtest_client):
        """Test candles from multiple instruments are merged in timestamp order."""
        inst1_id = self._create_instrument_and_candles(
            "MULTI_A", timestamps=[1000, 3000],
        )
        inst2_id = self._create_instrument_and_candles(
            "MULTI_B", timestamps=[2000, 4000],
        )

        backtest_client._end = 4000
        backtest_client.subscribe([
            {
                "symbol": "MULTI_A",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
            {
                "symbol": "MULTI_B",
                "market_type": MarketType.STOCKS,
                "broker_type": BrokerType.ALPACA,
                "timeframe": Timeframe.m1,
            },
        ])

        candles = list(backtest_client.candles())

        assert len(candles) == 4
        timestamps = [c.timestamp for c in candles]
        assert timestamps == sorted(timestamps)
        assert timestamps == [1000, 2000, 3000, 4000]

        self._cleanup([inst1_id, inst2_id])
