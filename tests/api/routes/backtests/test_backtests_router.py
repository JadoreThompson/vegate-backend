from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import delete

from api.routes.util import seed_candles
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
    generate_strategy_code = strategy_service._generate_strategy_code
    validate_strategy_code = strategy_service._validate_strategy_code
    strategy_service._generate_strategy_code = AsyncMock(
        return_value=StrategyGenOutput(
            name="Test strategy",
            description="A test strategy",
            code="print('Hello Im a test strategy')",
            error=None,
        )
    )
    strategy_service._validate_strategy_code = AsyncMock(return_value=True)

    rsp = await client.post("/strategy/", json={"description": prompt})
    rsp.raise_for_status()
    data = StrategyResponse(**rsp.json())

    strategy_service._generate_strategy_code = generate_strategy_code
    strategy_service._validate_strategy_code = validate_strategy_code

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
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:00Z",
            "timeframe": "1m",
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
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_missing_symbol_returns_422(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "strategy_id": str(strategy_id),
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1h",
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
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": -100,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1h",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_invalid_symbol_returns_404(
        self, authenticated_client
    ):
        strategy_id = uuid4()

        payload = {
            "strategy_id": str(strategy_id),
            "symbol": "FAKE",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 100,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1h",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 404

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_invalid_date_range_returns_400(
        self, authenticated_client
    ):
        strategy = await create_strategy(authenticated_client)
        strategy_id = strategy.id

        payload = {
            "strategy_id": str(strategy_id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 100,
            "start_date": "1970-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 400, res.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_invalid_timeframe_returns_400(
        self, authenticated_client
    ):
        strategy = await create_strategy(authenticated_client)
        strategy_id = strategy.id

        payload = {
            "strategy_id": str(strategy_id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 100,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1h",
        }

        res = await authenticated_client.post("/backtests/", json=payload)

        assert res.status_code == 404, res.json()


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
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        rsp = await authenticated_client.post("/backtests/", json=payload)

        assert rsp.status_code == 201, rsp.json()

        backtest_id = rsp.json()["id"]

        res = await authenticated_client.get(f"/backtests/{backtest_id}")

        assert res.status_code == 200
        data = res.json()
        assert "id" in data
        assert "metrics" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_not_found_returns_404(self, authenticated_client):
        fake_id = uuid4()
        res = await authenticated_client.get(f"/backtests/{fake_id}")

        assert res.status_code == 404


class TestListBacktests:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_backtests_returns_200(self, authenticated_client):
        res = await authenticated_client.get("/backtests/")
        assert res.status_code == 200
        assert len(res.json()["data"]) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_backtests_with_pagination(self, authenticated_client):
        strategy = await create_strategy(authenticated_client)

        from module.api.app import app

        api_backtests_service = app.state.object_registry.get(BacktestsService)
        api_backtests_service._backtest_executor.run = AsyncMock()

        seed_candles()

        request = {
            "strategy_id": str(strategy.id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:00Z",
            "timeframe": "1m",
        }

        for _ in range(10):
            await authenticated_client.post("/backtests/", json=request)

        res = await authenticated_client.get("/backtests/?page=1&limit=10")
        assert res.status_code == 200

        data = res.json()
        assert data["page"] == 1
        assert data["size"] == 10
        assert data["has_next"] == False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_backtests_with_status_filter(
        self, authenticated_client, db_sess
    ):
        strategy = await create_strategy(authenticated_client)

        from module.api.app import app

        api_backtests_service = app.state.object_registry.get(BacktestsService)
        api_backtests_service._backtest_executor.run = AsyncMock()

        request = {
            "strategy_id": str(strategy.id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        backtest_ids = []

        for _ in range(10):
            rsp = await authenticated_client.post("/backtests/", json=request)
            assert rsp.status_code == 201, rsp.json()
            backtest_ids.append(rsp.json()["id"])

        for i in range(5):
            backtest = await db_sess.get(Backtest, backtest_ids[i])
            backtest.status = BacktestStatus.COMPLETED
        await db_sess.commit()

        rsp = await authenticated_client.get("/backtests/?status=completed")
        assert rsp.status_code == 200

        data = rsp.json()
        assert data["page"] == 1
        assert data["size"] == 5
        assert data["has_next"] == False
        assert all(
            backtest["status"] == BacktestStatus.COMPLETED for backtest in data["data"]
        )


class TestDeleteBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_backtest_returns_204(self, authenticated_client, db_sess):
        strategy = await create_strategy(authenticated_client)

        from module.api.app import app

        api_backtests_service = app.state.object_registry.get(BacktestsService)
        api_backtests_service._backtest_executor.run = AsyncMock()

        request = {
            "strategy_id": str(strategy.id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        rsp = await authenticated_client.post("/backtests/", json=request)
        assert rsp.status_code == 201, rsp.json()

        backtest_id = rsp.json()["id"]
        backtest = await db_sess.get(Backtest, backtest_id)
        backtest.status = BacktestStatus.COMPLETED
        await db_sess.commit()

        res = await authenticated_client.delete(f"/backtests/{backtest_id}")

        assert res.status_code == 204

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_backtest_in_progress_returns_400(self, authenticated_client):
        strategy = await create_strategy(authenticated_client)

        from module.api.app import app

        api_backtests_service = app.state.object_registry.get(BacktestsService)
        api_backtests_service._backtest_executor.run = AsyncMock()

        request = {
            "strategy_id": str(strategy.id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T00:00:00Z",
            "end_date": "2026-01-15T23:59:59Z",
            "timeframe": "1m",
        }

        rsp = await authenticated_client.post("/backtests/", json=request)
        assert rsp.status_code == 201, rsp.json()

        res = await authenticated_client.delete(f"/backtests/{rsp.json()['id']}")

        assert res.status_code == 400


class TestGetBacktestOrders:

    @pytest.fixture
    def mock_backtest_executor_run(self):
        from module.api.app import app

        object_registry: ObjectRegistry = app.state.object_registry
        backtest_service = object_registry.get(BacktestsService)
        backtest_service._backtest_executor.run = AsyncMock()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_orders_returns_200(
        self, authenticated_client, mock_backtest_executor_run
    ):
        strategy = await create_strategy(authenticated_client)

        payload = {
            "strategy_id": str(strategy.id),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2026-01-01T03:00:00Z",
            "end_date": "2026-01-12T03:00:00Z",
            "timeframe": "1m",
        }

        res = await authenticated_client.post("/backtests/", json=payload)
        data = res.json()
        backtest_id = data["id"]

        res = await authenticated_client.get(f"/backtests/{backtest_id}/orders")

        assert res.status_code == 200

        data = res.json()
        assert "page" in data
        assert "size" in data
        assert "has_next" in data
        assert "data" in data

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_orders_with_pagination(self, authenticated_client):
        backtest_id = uuid4()

        res = await authenticated_client.get(
            f"/backtests/{backtest_id}/orders?page=1&limit=10"
        )

        assert res.status_code == 200


class TestBacktestEndpointsUnauthenticated:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_backtest_unauthenticated_returns_401(self, client):
        payload = {
            "strategy_id": str(uuid4()),
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
            "starting_balance": 10000,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "timeframe": "1h",
        }

        res = await client.post("/backtests/", json=payload)

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_list_backtests_unauthenticated_returns_401(self, client):
        res = await client.get("/backtests/")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_unauthenticated_returns_401(self, client):
        res = await client.get(f"/backtests/{uuid4()}")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_backtest_unauthenticated_returns_401(self, client):
        res = await client.delete(f"/backtests/{uuid4()}")

        assert res.status_code == 401

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_backtest_orders_unauthenticated_returns_401(self, client):
        res = await client.get(f"/backtests/{uuid4()}/orders")

        assert res.status_code == 401
