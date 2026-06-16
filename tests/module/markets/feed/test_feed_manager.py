import asyncio
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from vegate.oms.enums import BrokerType
from vegate.markets.enums import MarketType, Timeframe
from module.markets.feed.base import OHLCFeed
from module.markets.feed.manager import FeedManager


@pytest.fixture
def feed_manager():
    return FeedManager()


@pytest.fixture
def mock_feed():
    feed = MagicMock(spec=OHLCFeed)
    feed.symbols = ["AAPL"]
    feed.market_type = MarketType.STOCKS
    feed.broker = BrokerType.ALPACA
    feed.timeframes = [Timeframe.m1]
    feed.name = "MockFeed-AAPL"
    feed.stop = AsyncMock()
    return feed


@pytest.fixture
def mock_feed_crypto():
    feed = MagicMock(spec=OHLCFeed)
    feed.symbols = ["BTC/USD"]
    feed.market_type = MarketType.CRYPTO
    feed.broker = BrokerType.ALPACA
    feed.timeframes = [Timeframe.H1]
    feed.name = "MockFeed-BTC"
    feed.stop = AsyncMock()
    return feed


@pytest.fixture
def mock_feed_same_symbol_diff_timeframe():
    feed = MagicMock(spec=OHLCFeed)
    feed.symbols = ["AAPL"]
    feed.market_type = MarketType.STOCKS
    feed.broker = BrokerType.ALPACA
    feed.timeframes = [Timeframe.H1]
    feed.name = "MockFeed-AAPL-H1"
    feed.stop = AsyncMock()
    return feed


class TestGetSymbols:
    """Unit tests for get_symbols method."""

    def test_get_symbols_empty(self, feed_manager):
        assert feed_manager.get_symbols() == set()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols_after_register(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        symbols = feed_manager.get_symbols()
        assert symbols == {"AAPL"}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols_multiple(self, feed_manager, mock_feed, mock_feed_crypto):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_crypto)
        symbols = feed_manager.get_symbols()
        assert symbols == {"AAPL", "BTC/USD"}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols_same_symbol_diff_timeframe(
        self, feed_manager, mock_feed, mock_feed_same_symbol_diff_timeframe
    ):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_same_symbol_diff_timeframe)
        symbols = feed_manager.get_symbols()
        assert symbols == {"AAPL"}


class TestGetMarketTypes:
    """Unit tests for get_market_types method."""

    def test_get_market_types_empty(self, feed_manager):
        assert feed_manager.get_market_types("AAPL") == set()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_market_types_after_register(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        market_types = feed_manager.get_market_types("AAPL")
        assert market_types == {MarketType.STOCKS}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_market_types_multiple(self, feed_manager, mock_feed, mock_feed_crypto):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_crypto)

        stock_markets = feed_manager.get_market_types("AAPL")
        assert stock_markets == {MarketType.STOCKS}

        crypto_markets = feed_manager.get_market_types("BTC/USD")
        assert crypto_markets == {MarketType.CRYPTO}

    def test_get_market_types_unknown_symbol(self, feed_manager):
        assert feed_manager.get_market_types("UNKNOWN") == set()


class TestGetBrokers:
    """Unit tests for get_brokers method."""

    def test_get_brokers_empty(self, feed_manager):
        assert feed_manager.get_brokers("AAPL", MarketType.STOCKS) == set()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_brokers_after_register(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        brokers = feed_manager.get_brokers("AAPL", MarketType.STOCKS)
        assert brokers == {BrokerType.ALPACA}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_brokers_multiple_same_market(
        self, feed_manager, mock_feed
    ):
        mock_feed2 = MagicMock(spec=OHLCFeed)
        mock_feed2.symbols = ["AAPL"]
        mock_feed2.market_type = MarketType.STOCKS
        mock_feed2.broker = BrokerType.ALPACA  # Same broker
        mock_feed2.timeframes = [Timeframe.H1]
        mock_feed2.name = "MockFeed-AAPL-2"
        mock_feed2.stop = AsyncMock()

        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed2)

        brokers = feed_manager.get_brokers("AAPL", MarketType.STOCKS)
        assert brokers == {BrokerType.ALPACA}

    def test_get_brokers_unknown_symbol(self, feed_manager):
        assert feed_manager.get_brokers("UNKNOWN", MarketType.STOCKS) == set()

    def test_get_brokers_unknown_market(self, feed_manager):
        assert feed_manager.get_brokers("AAPL", MarketType.CRYPTO) == set()


class TestGetTimeframes:
    """Unit tests for get_timeframes method."""

    def test_get_timeframes_empty(self, feed_manager):
        assert (
            feed_manager.get_timeframes("AAPL", MarketType.STOCKS, BrokerType.ALPACA)
            == set()
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_timeframes_after_register(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        timeframes = feed_manager.get_timeframes(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA
        )
        assert timeframes == {Timeframe.m1}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_timeframes_multiple(
        self, feed_manager, mock_feed, mock_feed_same_symbol_diff_timeframe
    ):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_same_symbol_diff_timeframe)

        timeframes = feed_manager.get_timeframes(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA
        )
        assert timeframes == {Timeframe.m1, Timeframe.H1}

    def test_get_timeframes_unknown_symbol(self, feed_manager):
        assert (
            feed_manager.get_timeframes(
                "UNKNOWN", MarketType.STOCKS, BrokerType.ALPACA
            )
            == set()
        )

    def test_get_timeframes_unknown_market(self, feed_manager):
        assert (
            feed_manager.get_timeframes(
                "AAPL", MarketType.CRYPTO, BrokerType.ALPACA
            )
            == set()
        )

    def test_get_timeframes_unknown_broker(self, feed_manager):
        # This would require a different broker enum, but we test the empty case
        assert (
            feed_manager.get_timeframes(
                "AAPL", MarketType.STOCKS, BrokerType.ALPACA
            )
            == set()
        )


class TestRegister:
    """Unit tests for the register method."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_adds_feed(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        assert len(feed_manager._feeds) == 1
        assert feed_manager._feeds[0] == mock_feed

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_updates_symbol_map(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)

        timeframes = feed_manager._symbol_market_broker_timeframes["AAPL"][
            MarketType.STOCKS
        ][BrokerType.ALPACA]
        assert Timeframe.m1 in timeframes

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_logs_info(self, feed_manager, mock_feed, caplog):
        import logging
        with caplog.at_level(logging.INFO):
            await feed_manager.register(mock_feed)

        assert "Registered feed MockFeed-AAPL" in caplog.text

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_multiple_feeds(self, feed_manager, mock_feed, mock_feed_crypto):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_crypto)

        assert len(feed_manager._feeds) == 2
        symbols = feed_manager.get_symbols()
        assert symbols == {"AAPL", "BTC/USD"}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_same_symbol_diff_timeframe(
        self, feed_manager, mock_feed, mock_feed_same_symbol_diff_timeframe
    ):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_same_symbol_diff_timeframe)

        assert len(feed_manager._feeds) == 2
        timeframes = feed_manager.get_timeframes(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA
        )
        assert timeframes == {Timeframe.m1, Timeframe.H1}

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_raises_when_stopped(self, feed_manager, mock_feed):
        feed_manager._stopped = True

        with pytest.raises(ValueError, match="Manager has been stopped"):
            await feed_manager.register(mock_feed)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_does_not_add_when_stopped(self, feed_manager, mock_feed):
        feed_manager._stopped = True

        with pytest.raises(ValueError):
            await feed_manager.register(mock_feed)

        assert len(feed_manager._feeds) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_concurrent_access(self, feed_manager, mock_feed):
        """Test that concurrent registrations are handled safely."""
        async def register_feed(feed):
            await feed_manager.register(feed)

        feeds = [
            MagicMock(spec=OHLCFeed) for _ in range(10)
        ]
        for i, feed in enumerate(feeds):
            feed.symbols = [f"SYM{i}"]
            feed.market_type = MarketType.STOCKS
            feed.broker = BrokerType.ALPACA
            feed.timeframes = [Timeframe.m1]
            feed.name = f"Feed-{i}"
            feed.stop = AsyncMock()

        await asyncio.gather(*[register_feed(f) for f in feeds])

        assert len(feed_manager._feeds) == 10
        symbols = feed_manager.get_symbols()
        assert len(symbols) == 10


class TestStopAll:
    """Unit tests for the stop_all method."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_calls_stop_on_feeds(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        await feed_manager.stop_all()

        mock_feed.stop.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_calls_stop_on_all_feeds(
        self, feed_manager, mock_feed, mock_feed_crypto
    ):
        await feed_manager.register(mock_feed)
        await feed_manager.register(mock_feed_crypto)
        await feed_manager.stop_all()

        mock_feed.stop.assert_awaited_once()
        mock_feed_crypto.stop.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_continues_on_exception(self, feed_manager):
        bad_feed = MagicMock(spec=OHLCFeed)
        bad_feed.symbols = ["BAD"]
        bad_feed.market_type = MarketType.STOCKS
        bad_feed.broker = BrokerType.ALPACA
        bad_feed.timeframes = [Timeframe.m1]
        bad_feed.name = "BadFeed"
        bad_feed.stop = AsyncMock(side_effect=RuntimeError("Stop failed"))

        good_feed = MagicMock(spec=OHLCFeed)
        good_feed.symbols = ["GOOD"]
        good_feed.market_type = MarketType.STOCKS
        good_feed.broker = BrokerType.ALPACA
        good_feed.timeframes = [Timeframe.H1]
        good_feed.name = "GoodFeed"
        good_feed.stop = AsyncMock()

        await feed_manager.register(bad_feed)
        await feed_manager.register(good_feed)

        with pytest.raises(ExceptionGroup) as exc_info:
            await feed_manager.stop_all()

        assert "Received exceptions whilst stopping feeds" in str(exc_info.value)
        bad_feed.stop.assert_awaited_once()
        good_feed.stop.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_prevents_new_registrations(self, feed_manager, mock_feed):
        await feed_manager.stop_all()

        with pytest.raises(ValueError, match="Manager has been stopped"):
            await feed_manager.register(mock_feed)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_idempotent(self, feed_manager, mock_feed):
        await feed_manager.register(mock_feed)
        await feed_manager.stop_all()

        # Second stop should not raise
        await feed_manager.stop_all()

        assert feed_manager._stopped is True

class TestIntegration:
    """Integration tests for FeedManager."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_full_lifecycle(self, feed_manager):
        """Test register -> query -> stop lifecycle."""
        feed1 = MagicMock(spec=OHLCFeed)
        feed1.symbols = ["AAPL"]
        feed1.market_type = MarketType.STOCKS
        feed1.broker = BrokerType.ALPACA
        feed1.timeframes = [Timeframe.m1]
        feed1.name = "AAPL-m1"
        feed1.stop = AsyncMock()

        feed2 = MagicMock(spec=OHLCFeed)
        feed2.symbols = ["AAPL"]
        feed2.market_type = MarketType.STOCKS
        feed2.broker = BrokerType.ALPACA
        feed2.timeframes = [Timeframe.H1]
        feed2.name = "AAPL-H1"
        feed2.stop = AsyncMock()

        feed3 = MagicMock(spec=OHLCFeed)
        feed3.symbols = ["BTC/USD"]
        feed3.market_type = MarketType.CRYPTO
        feed3.broker = BrokerType.ALPACA
        feed3.timeframes = [Timeframe.m1]
        feed3.name = "BTC-m1"
        feed3.stop = AsyncMock()

        # Register all feeds
        await feed_manager.register(feed1)
        await feed_manager.register(feed2)
        await feed_manager.register(feed3)

        # Query structure
        assert feed_manager.get_symbols() == {"AAPL", "BTC/USD"}
        assert feed_manager.get_market_types("AAPL") == {MarketType.STOCKS}
        assert feed_manager.get_market_types("BTC/USD") == {MarketType.CRYPTO}
        assert feed_manager.get_brokers("AAPL", MarketType.STOCKS) == {BrokerType.ALPACA}
        assert feed_manager.get_timeframes("AAPL", MarketType.STOCKS, BrokerType.ALPACA) == {
            Timeframe.m1, Timeframe.H1
        }
        assert feed_manager.get_timeframes("BTC/USD", MarketType.CRYPTO, BrokerType.ALPACA) == {
            Timeframe.m1
        }

        # Stop all
        await feed_manager.stop_all()

        feed1.stop.assert_awaited_once()
        feed2.stop.assert_awaited_once()
        feed3.stop.assert_awaited_once()

        assert feed_manager._stopped is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_after_stop_raises(self, feed_manager):
        feed = MagicMock(spec=OHLCFeed)
        feed.symbols = ["TEST"]
        feed.market_type = MarketType.STOCKS
        feed.broker = BrokerType.ALPACA
        feed.timeframes = [Timeframe.m1]
        feed.name = "TestFeed"
        feed.stop = AsyncMock()

        await feed_manager.register(feed)
        await feed_manager.stop_all()

        new_feed = MagicMock(spec=OHLCFeed)
        new_feed.symbols = ["NEW"]
        new_feed.market_type = MarketType.STOCKS
        new_feed.broker = BrokerType.ALPACA
        new_feed.timeframes = [Timeframe.m1]
        new_feed.name = "NewFeed"
        new_feed.stop = AsyncMock()

        with pytest.raises(ValueError, match="Manager has been stopped"):
            await feed_manager.register(new_feed)