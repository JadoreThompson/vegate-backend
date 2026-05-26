import pytest
import pytest_asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy import delete

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.exception import SymbolNotFoundException
from module.markets.model import OHLC, Instrument
from module.markets.schema import InstrumentInfo
from module.markets.service import MarketsService
from core.db import get_db_session, get_db_sess_sync
from module.util import seed_candles


@pytest.fixture
def markets_service():
    return MarketsService()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestGetSymbolsInfo:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbols_info_returns_paginated_response(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_row = MagicMock()
            mock_row.id = uuid4()
            mock_row.symbol = "AAPL"
            mock_row.broker_type = BrokerType.ALPACA
            mock_row.market_type = MarketType.STOCKS
            mock_row.timeframe = Timeframe.m1
            mock_row.start_ts = 1700000000
            mock_row.end_ts = 1700003600

            mock_result = MagicMock()
            mock_result.all.return_value = [mock_row]
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_symbols_info(
                mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1
            assert isinstance(result.data[0], InstrumentInfo)
            assert result.data[0].symbol == "AAPL"

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbols_info_has_next_when_more_results(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_rows = []
            for i in range(11):
                row = MagicMock()
                row.id = uuid4()
                row.symbol = f"SYM{i}"
                row.broker_type = BrokerType.ALPACA
                row.market_type = MarketType.STOCKS
                row.timeframe = Timeframe.m1
                row.start_ts = 1700000000
                row.end_ts = 1700003600
                mock_rows.append(row)

            mock_result = MagicMock()
            mock_result.all.return_value = mock_rows
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_symbols_info(
                mock_db_sess, page=1, limit=10
            )

            assert result.has_next is True
            assert len(result.data) == 10
            assert result.size == 10

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbols_info_empty_returns_empty_list(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_symbols_info(
                mock_db_sess, page=1, limit=10
            )

            assert result.size == 0
            assert result.data == []
            assert result.has_next is False

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbols_info_filters_by_symbol(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_symbols_info(
                mock_db_sess, page=1, limit=10, symbol="AAPL"
            )

            assert mock_db_sess.execute.called
            assert result.size == 0
            assert result.data == []

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbols_info_returns_seeded_data(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_symbols_info(
                db_sess, page=1, limit=50
            )

            assert result.page == 1
            assert result.size >= 0
            assert len(result.data) == result.size
            for item in result.data:
                assert isinstance(item, InstrumentInfo)
                assert item.symbol is not None
                assert item.start_date is not None
                assert item.end_date is not None


class TestGetOHLCBars:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_returns_paginated_response(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_row = MagicMock()
            mock_row.open = 100.0
            mock_row.high = 101.0
            mock_row.low = 99.0
            mock_row.close = 100.5
            mock_row.volume = 1000.0
            mock_row.timestamp = 1700000000
            mock_row.timeframe = Timeframe.m1
            mock_row.symbol = "AAPL"
            mock_row.broker_type = BrokerType.ALPACA
            mock_row.market_type = MarketType.STOCKS

            mock_result = MagicMock()
            mock_result.all.return_value = [mock_row]
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_ohlc_bars(
                mock_db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=10,
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1
            assert result.data[0].symbol == "AAPL"
            assert result.data[0].broker == BrokerType.ALPACA
            assert result.data[0].market_type == MarketType.STOCKS
            assert result.data[0].timeframe == Timeframe.m1
            assert result.data[0].open == 100.0
            assert result.data[0].close == 100.5
            assert result.data[0].timestamp == 1700000000

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_has_next_when_more_results(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_rows = []
            for i in range(11):
                row = MagicMock()
                row.open = 100.0
                row.high = 101.0
                row.low = 99.0
                row.close = 100.5
                row.volume = 1000.0
                row.timestamp = 1700000000 + i
                row.timeframe = Timeframe.m1
                row.symbol = "AAPL"
                row.broker_type = BrokerType.ALPACA
                row.market_type = MarketType.STOCKS
                mock_rows.append(row)

            mock_result = MagicMock()
            mock_result.all.return_value = mock_rows
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_ohlc_bars(
                mock_db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=10,
            )

            assert result.has_next is True
            assert len(result.data) == 10
            assert result.size == 10

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_empty_returns_empty(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_ohlc_bars(
                mock_db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=10,
            )

            assert result.size == 0
            assert result.data == []
            assert result.has_next is False

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_no_last_page_has_next_false(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_rows = []
            for i in range(5):
                row = MagicMock()
                row.open = 100.0
                row.high = 101.0
                row.low = 99.0
                row.close = 100.5
                row.volume = 1000.0
                row.timestamp = 1700000000 + i
                row.timeframe = Timeframe.m1
                row.symbol = "AAPL"
                row.broker_type = BrokerType.ALPACA
                row.market_type = MarketType.STOCKS
                mock_rows.append(row)

            mock_result = MagicMock()
            mock_result.all.return_value = mock_rows
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_ohlc_bars(
                mock_db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=10,
            )

            assert result.has_next is False
            assert len(result.data) == 5
            assert result.size == 5

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_passes_filters(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_ohlc_bars(
                mock_db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=2,
                limit=50,
                start_time=1700000000,
                end_time=1700003600,
            )

            assert mock_db_sess.execute.called
            assert result.size == 0

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_returns_seeded_data(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_ohlc_bars(
                db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=50,
            )

            assert result.page == 1
            assert result.size > 0
            assert len(result.data) == result.size
            for item in result.data:
                assert item.symbol == "AAPL"
                assert item.broker == BrokerType.ALPACA
                assert item.market_type == MarketType.STOCKS
                assert item.timeframe == Timeframe.m1
                assert isinstance(item.open, float)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_with_time_filters(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_ohlc_bars(
                db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=50,
                start_time=int(datetime(2026, 1, 5, tzinfo=UTC).timestamp()),
                end_time=int(datetime(2026, 1, 10, tzinfo=UTC).timestamp()),
            )

            assert result.size > 0
            for item in result.data:
                assert item.timestamp >= int(datetime(2026, 1, 5, tzinfo=UTC).timestamp())
                assert item.timestamp <= int(datetime(2026, 1, 10, tzinfo=UTC).timestamp())

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_nonexistent_instrument_empty(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_ohlc_bars(
                db_sess,
                symbol="NONEXISTENT",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
            )

            assert result.size == 0
            assert result.data == []
            assert result.has_next is False

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_pagination_has_next(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_ohlc_bars(
                db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=1,
                limit=5,
            )

            assert result.size == 5
            assert result.has_next is True

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_ohlc_bars_second_page(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_ohlc_bars(
                db_sess,
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                page=2,
                limit=5,
            )

            assert result.size > 0
            assert result.page == 2


class TestGetSymbolInfo:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbol_info_not_found_raises(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.first.return_value = None
            mock_db_sess.execute.return_value = mock_result

            with pytest.raises(SymbolNotFoundException):
                await markets_service.get_symbol_info(
                    "FAKE", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, mock_db_sess
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbol_info_success(self, markets_service):
            mock_db_sess = AsyncMock()

            mock_row = MagicMock()
            mock_row.id = uuid4()
            mock_row.broker_type = BrokerType.ALPACA
            mock_row.timeframe = Timeframe.m1
            mock_row.market_type = MarketType.STOCKS
            mock_row.start_ts = 1700000000
            mock_row.end_ts = 1700003600

            mock_result = MagicMock()
            mock_result.first.return_value = mock_row
            mock_db_sess.execute.return_value = mock_result

            result = await markets_service.get_symbol_info(
                "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, mock_db_sess
            )

            assert isinstance(result, InstrumentInfo)
            assert result.symbol == "AAPL"
            assert result.broker_type == BrokerType.ALPACA
            assert result.market_type == MarketType.STOCKS
            assert result.timeframe == Timeframe.m1
            assert isinstance(result.start_date, datetime)
            assert isinstance(result.end_date, datetime)

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbol_info_returns_seeded_data(
            self, markets_service, db_sess
        ):
            seed_candles()
            result = await markets_service.get_symbol_info(
                "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, db_sess
            )

            assert isinstance(result, InstrumentInfo)
            assert result.symbol == "AAPL"
            assert result.broker_type == BrokerType.ALPACA
            assert result.market_type == MarketType.STOCKS
            assert result.timeframe == Timeframe.m1
            assert result.start_date is not None
            assert result.end_date is not None

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_symbol_info_nonexistent_symbol_raises(
            self, markets_service, db_sess
        ):
            with pytest.raises(SymbolNotFoundException):
                await markets_service.get_symbol_info(
                    "NONEXISTENT", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, db_sess
                )