import pytest
import pytest_asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy import delete

from api.routes.markets.exception import SymbolNotFoundException
from api.routes.markets.model import InstrumentInfo
from api.routes.markets.service import MarketsService
from enums import BrokerType, MarketType, Timeframe
from infra.db.model import OHLC
from infra.db.model.instrument import Instrument
from infra.db.utils import get_db_session, get_db_sess_sync
from api.routes.util import seed_candles


@pytest.fixture
def markets_service():
    return MarketsService()


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(OHLC))
        db_sess.execute(delete(Instrument))
        db_sess.commit()


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