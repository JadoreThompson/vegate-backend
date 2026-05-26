from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import delete

from module.util import seed_candles
from core.db import get_db_sess_sync, get_db_session
from core.redis import REDIS_CLIENT
from module.backtest.enums import BacktestStatus
from module.api.object_registry import ObjectRegistry
from module.auth import AuthService
from module.backtest import BacktestsService
from module.backtest.model import Backtest
from module.markets import MarketsService
from module.markets.model import OHLC
from module.strategy import StrategyService
from module.strategy.agents.strategy_gen import StrategyGenOutput
from module.strategy.schema import StrategyResponse


@pytest.fixture
def email_service():
    service = MagicMock()
    service.send_email = AsyncMock(return_value=None)
    return service


@pytest.fixture
def auth_service(email_service, monkeypatch):
    service = AuthService(email_service=email_service, redis_client=REDIS_CLIENT)
    monkeypatch.setattr("api.routes.auth.route.auth_service", service)
    yield service


@pytest.fixture
def strategy_service():
    return StrategyService()


@pytest.fixture
def markets_service():
    return MarketsService()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(Backtest))
        db_sess.commit()


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed():
    seed_candles()


async def create_strategy(
    client: AsyncClient, prompt: str = "Create a test strategy"
) -> StrategyResponse:
    from module.api.app import app

    object_registry = app.state.object_registry

    strategy_service = object_registry.get(StrategyService)

    rsp = await client.post(
        "/strategy/", json={"name": "Testing Strategy", "description": prompt}
    )
    rsp.raise_for_status()
    data = StrategyResponse(**rsp.json())

    return data


class TestCreateBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_returns_201(self, authenticated_client):
        strategy = await create_strategy(authenticated_client)
        strategy_id = strategy.id

        from module.api.app import app

        backtests_service = app.state.object_registry.get(BacktestsService)
        backtests_service._backtest_executor.run = AsyncMock()

        payload = {
            "strategy_id": str(strategy_id),
            # "symbol": "AAPL",
            # "broker": "alpaca",
            # "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:00Z",
            # "timeframe": "1m",
        }

        rsp = await authenticated_client.post("/backtests/", json=payload)

        assert rsp.status_code == 201, rsp.json()
        data = rsp.json()
        assert "id" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_missing_strategy_id_returns_422(
        self, authenticated_client
    ):
        payload = {
            # "symbol": "AAPL",
            # "broker": "alpaca",
            # "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            # "timeframe": "1m",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_invalid_balance_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "strategy_id": str(strategy_id),
            # "symbol": "AAPL",
            # "broker": "alpaca",
            # "market_type": "stocks",
            "starting_balance": -100,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            # "timeframe": "1h",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 422

class TestGetBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_returns_200(self, authenticated_client, db_sess):
        strategy = await create_strategy(authenticated_client)
        strategy_id = strategy.id

        from module.api.app import app

        api_backtests_service = app.state.object_registry.get(BacktestsService)
        api_backtests_service._backtest_executor.run = AsyncMock()

        payload = {
            "strategy_id": str(strategy_id),
            # "symbol": "AAPL",
            # "broker": "alpaca",
            # "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            # "timeframe": "1m",
        }

        rsp = await authenticated_client.post("/backtests/", json=payload)

        assert rsp.status_code == 201, rsp.json()

        backtest_id = rsp.json()["id"]

        res = await authenticated_client.get(f"/backtests/{backtest_id}")

        assert res.status_code == 200
        data = res.json()
        assert "id" in data
        assert "metrics" in data
