import pytest
import pytest_asyncio
from httpx import AsyncClient

from module.util import seed_candles
from core.db import get_db_sess_sync
from module.markets.model import OHLC, Instrument
from module.markets.service import MarketsService
from sqlalchemy import delete


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed():
    seed_candles()


class TestGetMarketsInfo:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_returns_200(self, client):
        res = await client.get("/api/v1/markets/info")
        assert res.status_code == 200
        data = res.json()
        assert "data" in data
        assert "page" in data
        assert "size" in data
        assert "has_next" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_with_pagination(self, client):
        res = await client.get("/api/v1/markets/info?page=1&limit=10")
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 1
        assert data["size"] <= 10

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_second_page(self, client):
        res = await client.get("/api/v1/markets/info?page=2&limit=10")
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_with_symbol_filter(self, client):
        res = await client.get("/api/v1/markets/info?symbol=AAPL")
        assert res.status_code == 200
        data = res.json()
        assert all(item["symbol"] == "AAPL" for item in data["data"])

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_invalid_page_returns_422(self, client):
        res = await client.get("/api/v1/markets/info?page=0")
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_invalid_limit_returns_422(self, client):
        res = await client.get("/api/v1/markets/info?limit=101")
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_markets_info_nonexistent_symbol_returns_empty(self, client):
        res = await client.get("/api/v1/markets/info?symbol=FAKE_SYMBOL_12345")
        assert res.status_code == 200
        data = res.json()
        assert data["size"] == 0
        assert data["data"] == []
        assert data["has_next"] is False


class TestGetOHLCBars:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_returns_200(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&market_type=stocks&broker_type=alpaca&timeframe=1m"
        )
        assert res.status_code == 200
        data = res.json()
        assert "data" in data
        assert "page" in data
        assert "size" in data
        assert "has_next" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_with_pagination(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&market_type=stocks&broker_type=alpaca&timeframe=1m&page=1&limit=10"
        )
        assert res.status_code == 200
        data = res.json()
        assert data["page"] == 1
        assert data["size"] <= 10

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_with_time_filters(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&market_type=stocks&broker_type=alpaca&timeframe=1m&start_time=1700000000&end_time=1700003600"
        )
        assert res.status_code == 200
        data = res.json()
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_missing_symbol_returns_422(self, client):
        res = await client.get(
            "/api/v1/markets/bars?market_type=stocks&broker_type=alpaca&timeframe=1m"
        )
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_missing_market_type_returns_422(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&broker_type=alpaca&timeframe=1m"
        )
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_invalid_page_returns_422(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&market_type=stocks&broker_type=alpaca&timeframe=1m&page=0"
        )
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_invalid_limit_returns_422(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=AAPL&market_type=stocks&broker_type=alpaca&timeframe=1m&limit=201"
        )
        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_ohlc_bars_nonexistent_instrument_returns_empty(self, client):
        res = await client.get(
            "/api/v1/markets/bars?symbol=FAKE_SYMBOL_12345&market_type=stocks&broker_type=alpaca&timeframe=1m"
        )
        assert res.status_code == 200
        data = res.json()
        assert data["size"] == 0
        assert data["data"] == []
        assert data["has_next"] is False