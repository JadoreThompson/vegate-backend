from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from module.util import create_user
from core.db import get_db_sess_sync, get_db_session
from module.backtest.enums import BacktestStatus
from module.broker.enums import BrokerType, OrderSide, OrderStatus, OrderType
from module.markets.enums import MarketType, Timeframe
from module.backtest import BacktestsService
from module.backtest.model import Backtest, BacktestMetrics, BacktestOrder
from module.backtest.exception import (
    BacktestInProgressException,
    BacktestNotFoundException,
)
from module.backtest.schema import BacktestResponse, CreateBacktestRequest
from module.markets.model import Instrument
from module.strategy.exception import (
    StrategyNotFoundException,
    StrategyVersionNotFoundException,
)
from module.strategy import StrategyService
from module.strategy.model import Strategy, StrategyVersion


@pytest.fixture
def mock_strategy_service():
    return MagicMock(spec=StrategyService)


@pytest.fixture
def mock_backtest_executor():
    service = AsyncMock()
    service.run = AsyncMock()
    service.stop = AsyncMock()
    return service


@pytest.fixture
def mock_event_publisher():
    publisher = AsyncMock()
    return publisher


@pytest.fixture
def mock_markets_service():
    service = MagicMock()
    return service


@pytest.fixture
def backtest_service(
    mock_strategy_service, mock_backtest_executor, mock_markets_service, mock_event_publisher
):
    return BacktestsService(
        strategy_service=mock_strategy_service,
        # backtest_executor=mock_backtest_executor,
        event_publisher=mock_event_publisher,
        markets_service=mock_markets_service,
    )


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(BacktestOrder))
        db_sess.execute(delete(BacktestMetrics))
        db_sess.execute(delete(Backtest))
        db_sess.execute(delete(Strategy))
        db_sess.commit()


@pytest_asyncio.fixture
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestCreateBacktest:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_version_not_found_raises(
            self, backtest_service, mock_strategy_service
        ):
            mock_db_sess = AsyncMock()

            mock_strategy_service.get_user_strategy_version.side_effect = (
                StrategyVersionNotFoundException()
            )

            request = CreateBacktestRequest(
                version_id=uuid4(),
                starting_balance=10000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            with pytest.raises(StrategyVersionNotFoundException):
                await backtest_service.create(request, uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_success(
            self,
            backtest_service,
            mock_strategy_service,
            # mock_backtest_executor,
            mock_event_publisher
        ):
            mock_db_sess = AsyncMock()

            mock_strategy_service.get_user_strategy_version = AsyncMock()

            mock_result = MagicMock()
            mock_result.id = uuid4()
            mock_result.first.return_value = True
            mock_db_sess.execute.return_value = mock_result

            mock_db_sess.add = MagicMock()
            mock_db_sess.flush = AsyncMock()
            mock_db_sess.refresh = AsyncMock()

            # mock_backtest_executor.run_backtest = AsyncMock()

            request = CreateBacktestRequest(
                version_id=uuid4(),
                starting_balance=10000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

            def _Backtest(**kw):
                ns = SimpleNamespace(**kw)
                ns.id = uuid4()
                return ns

            with patch("module.backtest.service.Backtest", _Backtest):
                result = await backtest_service.create(request, uuid4(), mock_db_sess)

            assert result is not None
            mock_db_sess.add.assert_called_once()
            mock_event_publisher.publish.assert_awaited_once()


class TestGetBacktest:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_backtest_not_found_raises(self, backtest_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(BacktestNotFoundException):
                await backtest_service.get_user_backtest(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_backtest_success(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.id = uuid4()
            mock_backtest.version_id = uuid4()
            mock_backtest.starting_balance = 10000
            mock_backtest.start_date = date(2024, 1, 1)
            mock_backtest.end_date = date(2024, 12, 31)
            mock_backtest.status = BacktestStatus.COMPLETED
            mock_backtest.created_at = MagicMock()

            mock_metrics = MagicMock()
            mock_metrics.realised_pnl = 1000.0
            mock_metrics.unrealised_pnl = 500.0
            mock_metrics.total_return_pct = 15.0
            mock_metrics.profit_factor = 1.5
            mock_metrics.total_orders = 10

            mock_db_sess.scalar.side_effect = [mock_backtest, mock_metrics]

            result = await backtest_service.get_backtest(uuid4(), uuid4(), mock_db_sess)

            assert isinstance(result, BacktestResponse)
            assert result.status == BacktestStatus.COMPLETED


class TestGetBacktests:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_backtests_returns_paginated_response(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.id = uuid4()
            mock_backtest.version_id = uuid4()
            mock_backtest.starting_balance = 10000
            mock_backtest.start_date = date(2024, 1, 1)
            mock_backtest.end_date = date(2024, 12, 31)
            mock_backtest.status = BacktestStatus.COMPLETED
            mock_backtest.created_at = MagicMock()

            mock_metrics = MagicMock()
            mock_metrics.realised_pnl = 1000.0
            mock_metrics.unrealised_pnl = 500.0
            mock_metrics.total_return_pct = 15.0
            mock_metrics.profit_factor = 1.5
            mock_metrics.total_orders = 10

            mock_result = MagicMock()
            mock_result.all.return_value = [(mock_backtest, mock_metrics)]
            mock_db_sess.execute.return_value = mock_result

            result = await backtest_service.get_backtests(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1


class TestDeleteBacktest:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_in_progress_raises(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.status = BacktestStatus.IN_PROGRESS
            mock_db_sess.scalar.return_value = mock_backtest

            with pytest.raises(BacktestInProgressException):
                await backtest_service.delete(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_success(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.status = BacktestStatus.COMPLETED

            mock_db_sess.scalar.return_value = mock_backtest
            mock_db_sess.delete = AsyncMock()

            await backtest_service.delete(uuid4(), uuid4(), mock_db_sess)

            mock_db_sess.delete.assert_called_once_with(mock_backtest)


class TestGetOrders:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_orders_returns_paginated_response(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_order = BacktestOrder(
                id=uuid4(),
                backtest_id=uuid4(),
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10.0,
                filled_quantity=10.0,
                limit_price=None,
                stop_price=None,
                avg_fill_price=100.0,
                status=OrderStatus.FILLED,
                submitted_at=datetime(year=2024, month=1, day=1),
                filled_at=datetime(year=2024, month=1, day=1),
                details=None,
            )

            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_order]

            mock_db_sess.execute.return_value = mock_result

            result = await backtest_service.get_orders(
                uuid4(), uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1


class TestGetByVersionId:
    
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_returns_paginated_response(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.id = uuid4()
            mock_backtest.version_id = uuid4()
            mock_backtest.starting_balance = 10000
            mock_backtest.start_date = date(2024, 1, 1)
            mock_backtest.end_date = date(2024, 12, 31)
            mock_backtest.status = BacktestStatus.COMPLETED
            mock_backtest.created_at = MagicMock()

            mock_metrics = MagicMock()
            mock_metrics.realised_pnl = 1000.0
            mock_metrics.unrealised_pnl = 500.0
            mock_metrics.total_return_pct = 15.0
            mock_metrics.profit_factor = 1.5
            mock_metrics.total_orders = 10
            mock_metrics.equity_curve = []

            mock_result = MagicMock()
            mock_result.all.return_value = [(mock_backtest, mock_metrics)]
            mock_db_sess.execute.return_value = mock_result

            result = await backtest_service.get_by_version_id(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert result.has_next is False
            assert len(result.data) == 1

            response = result.data[0]
            assert response.id == mock_backtest.id
            assert response.version_id == mock_backtest.version_id
            assert response.status == BacktestStatus.COMPLETED

        @pytest.mark.asyncio(loop_scope="session")
        async def test_returns_empty_when_no_backtests(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await backtest_service.get_by_version_id(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 0
            assert result.has_next is False
            assert len(result.data) == 0


class TestGetUserBacktest:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_backtest_not_found_raises(self, backtest_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(BacktestNotFoundException):
                await backtest_service.get_user_backtest(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_backtest_returns_backtest(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_db_sess.scalar.return_value = mock_backtest

            result = await backtest_service.get_user_backtest(
                uuid4(), uuid4(), mock_db_sess
            )

            assert result == mock_backtest


class TestIntegrationTests:

    @pytest_asyncio.fixture(loop_scope="session")
    async def instrument_id(self):
        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                pg_insert(Instrument)
                .values(
                    symbol="AAPL",
                    native_symbol="AAPL",
                    broker_type=BrokerType.ALPACA,
                    market_type=MarketType.STOCKS,
                )
                .on_conflict_do_update(
                    index_elements=["symbol", "market_type", "broker_type"],
                    set_={
                        "symbol": Instrument.symbol,
                    },
                )
                .returning(Instrument.id)
            )

            instrument_id = res.scalar_one()

            await db_sess.commit()

            return instrument_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_delete_completed_backtest_removes_from_db(
        self,
        backtest_service,
        mock_strategy_service,
        mock_backtest_executor,
        db_sess,
        instrument_id,
    ):
        user = await create_user("test-delete-backtest-user-2")
        user_id = user.user_id
        strategy_id = uuid4()

        strategy = Strategy(
            user_id=user_id,
            name="Delete Test Strategy",
            description="Test description",
        )
        db_sess.add(strategy)
        await db_sess.flush()

        version = StrategyVersion(strategy_id=strategy.strategy_id)
        db_sess.add(version)
        await db_sess.flush()

        backtest = Backtest(
            version_id=version.id,
            starting_balance=10000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            status=BacktestStatus.COMPLETED,
        )
        db_sess.add(backtest)
        await db_sess.commit()

        backtest_id = backtest.id

        async with get_db_session() as new_db_sess:
            await backtest_service.delete(backtest_id, user_id, new_db_sess)
            await new_db_sess.commit()

        async with get_db_session() as new_db_sess:
            deleted = await new_db_sess.get(Backtest, backtest_id)

        assert deleted is None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_orders_returns_user_orders_only(
        self,
        backtest_service,
        mock_strategy_service,
        mock_backtest_executor,
        db_sess,
        instrument_id,
    ):
        user = await create_user("test-get-orders-user-2")
        user_id = user.user_id

        other_user = await create_user("test-get-orders-other-user-3")
        other_user_id = other_user.user_id

        strategy = Strategy(
            user_id=user_id,
            name="Orders Test Strategy",
            description="Test description",
        )
        db_sess.add(strategy)

        other_strategy = Strategy(
            user_id=other_user_id,
            name="Other Strategy",
            description="Other description",
        )
        db_sess.add(other_strategy)

        await db_sess.flush()

        version = StrategyVersion(strategy_id=strategy.strategy_id)
        db_sess.add(version)
        other_version = StrategyVersion(strategy_id=other_strategy.strategy_id)
        db_sess.add(other_version)
        await db_sess.flush()

        backtest = Backtest(
            version_id=version.id,
            starting_balance=10000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            status=BacktestStatus.COMPLETED,
        )
        db_sess.add(backtest)

        other_backtest = Backtest(
            version_id=other_version.id,
            starting_balance=5000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            status=BacktestStatus.COMPLETED,
        )
        db_sess.add(other_backtest)

        await db_sess.flush()

        user_order = BacktestOrder(
            backtest_id=backtest.id,
            symbol="AAPL",
            side="buy",
            order_type="market",
            quantity=10.0,
            filled_quantity=10.0,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 1, 1),
        )
        db_sess.add(user_order)

        other_user_order = BacktestOrder(
            backtest_id=other_backtest.id,
            symbol="MSFT",
            side="sell",
            order_type="market",
            quantity=5.0,
            filled_quantity=5.0,
            status=OrderStatus.FILLED,
            submitted_at=datetime(2024, 1, 2),
        )
        db_sess.add(other_user_order)

        await db_sess.commit()

        async with get_db_session() as new_db_sess:
            result = await backtest_service.get_orders(
                backtest.id,
                user_id,
                new_db_sess,
                page=1,
                limit=10,
            )

        assert len(result.data) == 1

        returned_order = result.data[0]

        assert returned_order.id == user_order.id
        assert returned_order.symbol == "AAPL"

        returned_ids = {order.id for order in result.data}
        assert other_user_order.id not in returned_ids
