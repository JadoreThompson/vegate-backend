import asyncio
import pytest
import pytest_asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from sqlalchemy import delete, select

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from vegate.oms.enums import BrokerType
from vegate.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC, Instrument
from core.db import get_db_session, get_db_sess_sync
from vegate.markets.schema import OHLC as OHLCSchema
from module.markets.feed.alpaca.service import AlpacaOHLCFeed


@pytest.fixture
def alpaca_feed():
    return AlpacaOHLCFeed(
        symbol="AAPL",
        market_type=MarketType.STOCKS,
        timeframe=Timeframe.m1,
        api_key="test-api-key",
        secret_key="test-secret-key",
    )


@pytest.fixture
def alpaca_feed_crypto():
    return AlpacaOHLCFeed(
        symbol="BTC/USD",
        market_type=MarketType.CRYPTO,
        timeframe=Timeframe.H1,
        api_key="test-api-key",
        secret_key="test-secret-key",
    )


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestProperties:
    """Unit tests for AlpacaOHLCFeed properties."""

    def test_name_property(self, alpaca_feed):
        assert alpaca_feed.name == "AlpacaOHLCFeed-AAPL-stocks"

    def test_market_type_property(self, alpaca_feed):
        assert alpaca_feed.market_type == MarketType.STOCKS

    def test_symbol_property(self, alpaca_feed):
        assert alpaca_feed.symbol == "AAPL"

    def test_broker_property(self, alpaca_feed):
        assert alpaca_feed.broker == BrokerType.ALPACA

    def test_timeframe_property(self, alpaca_feed):
        assert alpaca_feed.timeframe == Timeframe.m1


class TestGetUrl:
    """Unit tests for WebSocket URL generation."""

    def test_get_url_stocks(self, alpaca_feed):
        url = alpaca_feed._get_url()
        assert url == "wss://stream.data.alpaca.markets/v2/iex"

    def test_get_url_crypto(self, alpaca_feed_crypto):
        url = alpaca_feed_crypto._get_url()
        assert url == "wss://stream.data.alpaca.markets/v1beta3/crypto/eu-1"


class TestGenerateSubscriptionMessage:
    """Unit tests for subscription message generation."""

    def test_subscribe_bars_intraday(self, alpaca_feed):
        msg = alpaca_feed._generate_subscription_message()
        assert msg["action"] == "subscribe"
        assert msg["bars"] == ["AAPL"]
        assert "dailyBars" not in msg

    def test_subscribe_daily_bars_daily_timeframe(self, alpaca_feed_crypto):
        # H1 is less than 86400 seconds, so should use bars
        msg = alpaca_feed_crypto._generate_subscription_message()
        assert msg["action"] == "subscribe"
        assert msg["bars"] == ["BTC/USD"]

    def test_subscribe_daily_bars_for_d1(self):
        feed = AlpacaOHLCFeed(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            timeframe=Timeframe.D1,
            api_key="key",
            secret_key="secret",
        )
        msg = feed._generate_subscription_message()
        assert msg["action"] == "subscribe"
        assert msg["dailyBars"] == ["AAPL"]
        assert "bars" not in msg


class TestParseCandle:
    """Unit tests for candle parsing from Alpaca format."""

    def test_parse_candle_success(self, alpaca_feed):
        candle_data = {
            "o": 100.0,
            "h": 105.0,
            "l": 99.0,
            "c": 102.0,
            "v": 1000,
            "t": "2024-01-01T10:00:00Z",
        }

        result = alpaca_feed._parse_candle(candle_data)

        assert isinstance(result, OHLCSchema)
        assert result.open == 100.0
        assert result.high == 105.0
        assert result.low == 99.0
        assert result.close == 102.0
        assert result.volume == 1000
        assert result.symbol == "AAPL"
        assert result.broker == BrokerType.ALPACA
        assert result.market_type == MarketType.STOCKS
        assert result.timeframe == Timeframe.m1
        assert result.timestamp == int(
            datetime(2024, 1, 1, 10, 0, tzinfo=UTC).timestamp()
        )

    def test_parse_candle_different_timeframe(self):
        feed = AlpacaOHLCFeed(
            symbol="BTC/USD",
            market_type=MarketType.CRYPTO,
            timeframe=Timeframe.H1,
            api_key="key",
            secret_key="secret",
        )
        candle_data = {
            "o": 50000.0,
            "h": 51000.0,
            "l": 49000.0,
            "c": 50500.0,
            "v": 50000,
            "t": "2024-06-15T14:00:00+00:00",
        }

        result = feed._parse_candle(candle_data)

        assert result.timeframe == Timeframe.H1
        assert result.symbol == "BTC/USD"
        assert result.market_type == MarketType.CRYPTO


class TestSetOnCandle:
    """Unit tests for setting the candle callback."""

    def test_set_on_candle_sync_function(self, alpaca_feed):
        def callback(candle):
            pass

        alpaca_feed.set_on_candle(callback)
        assert alpaca_feed._on_candle is callback

    def test_set_on_candle_async_function(self, alpaca_feed):
        async def callback(candle):
            pass

        alpaca_feed.set_on_candle(callback)
        assert alpaca_feed._on_candle is callback

    def test_set_on_candle_none(self, alpaca_feed):
        alpaca_feed.set_on_candle(None)
        assert alpaca_feed._on_candle is None


class TestPersistCandle:
    """Unit tests for persisting candles to database."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_candle_success(self, alpaca_feed, db_sess):
        # Create instrument first
        instrument = Instrument(
            symbol="PERSTEST",
            native_symbol="PERSTEST",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        alpaca_feed._instrument_id = instrument.id

        candle = OHLCSchema(
            open=100.0,
            high=105.0,
            low=99.0,
            close=102.0,
            symbol="PERSTEST",
            volume=1000,
            broker=BrokerType.ALPACA,
            market_type=MarketType.STOCKS,
            timestamp=int(datetime(2024, 1, 1, 10, 0).timestamp()),
            timeframe=Timeframe.H1,
        )

        await alpaca_feed._persist_candle(candle)

        # Verify record exists
        async with get_db_session() as new_sess:
            res = await new_sess.execute(
                select(OHLC).where(OHLC.instrument_id == instrument.id)
            )
            records = res.scalars().all()
            assert len(records) == 1
            assert records[0].open == 100.0
            assert records[0].high == 105.0
            assert records[0].low == 99.0
            assert records[0].close == 102.0
            assert records[0].volume == 1000
            assert records[0].timeframe == Timeframe.H1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_candle_multiple(self, alpaca_feed, db_sess):
        instrument = Instrument(
            symbol="PERSTEST2",
            native_symbol="PERSTEST2",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        alpaca_feed._instrument_id = instrument.id

        for i in range(3):
            candle = OHLCSchema(
                open=100.0 + i,
                high=105.0 + i,
                low=99.0 + i,
                close=102.0 + i,
                symbol="PERSTEST2",
                volume=1000 + i,
                broker=BrokerType.ALPACA,
                market_type=MarketType.STOCKS,
                timestamp=int(datetime(2024, 1, 1, 10 + i, 0).timestamp()),
                timeframe=Timeframe.H1,
            )
            await alpaca_feed._persist_candle(candle)

        async with get_db_session() as new_sess:
            res = await new_sess.execute(
                select(OHLC).where(OHLC.instrument_id == instrument.id)
            )
            records = res.scalars().all()
            assert len(records) == 3


class TestIntegration:
    """Integration tests for AlpacaOHLCFeed with real database."""

    @pytest.mark.skip("Requires pro plan to subscribe to multiple feeds")
    @pytest.mark.asyncio(loop_scope="session")
    async def test_subscribe_success(self):
        alpaca_feed = AlpacaOHLCFeed(
            symbol="SOL/USD",
            market_type=MarketType.CRYPTO,
            timeframe=Timeframe.m1,
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_SECRET_KEY,
        )

        alpaca_feed._persist_candle = AsyncMock()

        candles = []

        def func(candle):
            candles.append(candle)

        alpaca_feed.set_on_candle(func)

        try:
            await asyncio.wait_for(alpaca_feed.run(), timeout=61)
        except asyncio.TimeoutError:
            pass

        assert len(candles) <= 2
