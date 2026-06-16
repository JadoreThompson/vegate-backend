import asyncio
import pytest
import pytest_asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from sqlalchemy import select

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from core.db import get_db_session
from module.markets.model import OHLC, Instrument
from module.markets.feed.alpaca import AlpacaOHLCFeed
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.oms.enums import BrokerType


@pytest.fixture
def alpaca_feed():
    return AlpacaOHLCFeed(
        market_type=MarketType.STOCKS,
        instruments=[("AAPL", [Timeframe.m1])],
        api_key="test-api-key",
        secret_key="test-secret-key",
    )


@pytest.fixture
def alpaca_feed_crypto():
    return AlpacaOHLCFeed(
        market_type=MarketType.CRYPTO,
        instruments=[("BTC/USD", [Timeframe.H1])],
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
        assert "AlpacaOHLCFeed" in alpaca_feed.name
        assert "stocks" in alpaca_feed.name

    def test_market_type_property(self, alpaca_feed):
        assert alpaca_feed.market_type == MarketType.STOCKS

    def test_symbols_property(self, alpaca_feed):
        assert alpaca_feed.symbols == ["AAPL"]

    def test_broker_property(self, alpaca_feed):
        assert alpaca_feed.broker == BrokerType.ALPACA

    def test_timeframes_property(self, alpaca_feed):
        assert alpaca_feed.timeframes == [Timeframe.m1]


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
            market_type=MarketType.STOCKS,
            instruments=[("AAPL", [Timeframe.D1])],
            api_key="key",
            secret_key="secret",
        )
        msg = feed._generate_subscription_message()
        assert msg["action"] == "subscribe"
        assert msg["dailyBars"] == ["AAPL"]
        assert "bars" not in msg


class TestRawToSchema:
    """Unit tests for _raw_to_schema."""

    def test_raw_to_schema_success(self, alpaca_feed):
        candle_data = {
            "o": 100.0,
            "h": 105.0,
            "l": 99.0,
            "c": 102.0,
            "v": 1000,
            "t": "2024-01-01T10:00:00Z",
        }

        result = alpaca_feed._raw_to_schema(candle_data, Timeframe.m1, "AAPL")

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

    def test_raw_to_schema_different_timeframe(self):
        feed = AlpacaOHLCFeed(
            market_type=MarketType.CRYPTO,
            instruments=[("BTC/USD", [Timeframe.H1])],
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

        result = feed._raw_to_schema(candle_data, Timeframe.H1, "BTC/USD")

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
