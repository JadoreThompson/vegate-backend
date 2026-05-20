import pytest
import pytest_asyncio
from httpx import AsyncClient

from api.routes.markets.service import MarketsService
from api.routes.util import seed_candles
from infra.db import get_db_sess_sync
from infra.db.model import OHLC
from infra.db.model.instrument import Instrument
from sqlalchemy import delete


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(OHLC))
        db_sess.execute(delete(Instrument))
        db_sess.commit()


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed():
    seed_candles()


class TestGetMarketsInfo:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_returns_200(self, client):
        res = await client.get("/markets/info")
        assert res.status_code == 200
        data = res.json()
        assert "data" in data
        assert "page" in data
        assert "size" in data
        assert "has_next" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_with_pagination(self, client):
        res = await client.get("/markets/info?page=1&limit=10")
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 1
        assert data["size"] <= 10

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_second_page(self, client):
        res = await client.get("/markets/info?page=2&limit=10")
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_with_symbol_filter(self, client):
        res = await client.get("/markets/info?symbol=AAPL")
        assert res.status_code == 200
        data = res.json()
        assert all(item["symbol"] == "AAPL" for item in data["data"])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_invalid_page_returns_422(self, client):
        res = await client.get("/markets/info?page=0")
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_invalid_limit_returns_422(self, client):
        res = await client.get("/markets/info?limit=101")
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_nonexistent_symbol_returns_empty(self, client):
        res = await client.get("/markets/info?symbol=FAKE_SYMBOL_12345")
        assert res.status_code == 200
        data = res.json()
        assert data["size"] == 0
        assert data["data"] == []
        assert data["has_next"] is False