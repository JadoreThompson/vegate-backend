import pytest
import pytest_asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.markets.service import MarketsService
from enums import BrokerType, MarketType, Timeframe
from infra.db.model.ohlc import OHLC
from infra.db.utils import get_db_sess_sync, get_db_session


@pytest.fixture
def markets_service():
    return MarketsService()


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(OHLC))
        db_sess.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed(clear_table):
    symbol_broker_tfs = (("AAPL", BrokerType.ALPACA, Timeframe.m1), ("AAPL", BrokerType.ALPACA, Timeframe.m5))

    with get_db_sess_sync() as db_sess:
        for symbol, broker, tf in symbol_broker_tfs:
            candles = [
                OHLC(
                    source=broker,
                    symbol=symbol,
                    timeframe=tf,
                    market_type=MarketType.STOCKS,
                    open=100.0,
                    high=100.0,
                    low=100.0,
                    close=100.0,
                    volume=10.0,
                    timestamp=int(datetime(year=2026, month=1, day=((1 + i) % 30) + 1, tzinfo=UTC).timestamp()),
                )
                for i in range(100)
            ]
            db_sess.add_all(candles)

        db_sess.commit()


class TestGetSymbolsInfo:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols_info_with_non_existent_symbol(self, db_sess, markets_service):
        info = await markets_service.get_symbol_info("GOOG", db_sess)
        assert len(info) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols_info_with_existent_symbol(self, db_sess, markets_service):
        info = await markets_service.get_symbol_info("AAPL", db_sess)
        assert len(info) == 2

        assert info[0].symbol == "AAPL"
        assert info[0].market_type == MarketType.STOCKS
        assert info[0].timeframe == Timeframe.m1

        assert info[1].symbol == "AAPL"
        assert info[1].timeframe == Timeframe.m5
        assert info[1].market_type == MarketType.STOCKS

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_symbols(self, db_sess, markets_service):
        info = await markets_service.get_symbol_info("AAPL", db_sess)
        assert len(info) == 2

        assert info[0].symbol == "AAPL"
        assert info[0].timeframe == Timeframe.m1

        assert info[1].symbol == "AAPL"
        assert info[1].timeframe == Timeframe.m5
