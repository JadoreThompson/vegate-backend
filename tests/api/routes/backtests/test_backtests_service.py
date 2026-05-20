from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.routes.backtests.exception import (
    BacktestInProgressError,
    BacktestNotFoundException,
)
from api.routes.backtests.model import BacktestResponse, CreateBacktestRequest
from api.routes.backtests.service import APIBacktestsService
from api.routes.broker_connections.test_broker_connections_service import create_user
from api.routes.markets.exception import SymbolNotFoundException
from api.routes.markets.model import InstrumentInfo
from api.routes.strategy.exception import StrategyNotFoundException
from api.routes.strategy.service import APIStrategyService
from enums import (
    BacktestStatus,
    BrokerType,
    MarketType,
    OrderSide,
    OrderStatus,
    OrderType,
    Timeframe,
)
from infra.db.model import (
    Backtest,
    BacktestMetrics,
    BacktestOrder,
    Strategy,
)
from infra.db.model.instrument import Instrument
from infra.db.utils import get_db_session, get_db_sess_sync


@pytest.fixture
def mock_strategy_service():
    return MagicMock(spec=APIStrategyService)


@pytest.fixture
def mock_backtest_runner_service():
    service = AsyncMock()
    service.run_backtest = AsyncMock()
    service.stop_backtest = AsyncMock()
    return service


@pytest.fixture
def mock_markets_service():
    service = MagicMock()
    return service


@pytest.fixture
def backtest_service(
    mock_strategy_service, mock_backtest_runner_service, mock_markets_service
):
    return APIBacktestsService(
        strategy_service=mock_strategy_service,
        backtest_service=mock_backtest_runner_service,
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
        async def test_create_strategy_not_found_raises(
            self, backtest_service, mock_strategy_service
        ):
            mock_db_sess = AsyncMock()

            mock_strategy_service.get_user_strategy.side_effect = (
                StrategyNotFoundException()
            )

            request = CreateBacktestRequest(
                strategy_id=uuid4(),
                symbol="AAPL",
                broker=BrokerType.ALPACA,
                market_type=MarketType.STOCKS,
                starting_balance=10000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                timeframe=Timeframe.H1,
            )

            with pytest.raises(StrategyNotFoundException):
                await backtest_service.create(request, uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_symbol_not_found_raises(
            self, backtest_service, mock_strategy_service, mock_markets_service
        ):
            mock_db_sess = AsyncMock()

            mock_strategy_service.get_user_strategy = AsyncMock()

            mock_result = MagicMock()
            mock_result.first.return_value = False
            mock_db_sess.execute.return_value = mock_result

            request = CreateBacktestRequest(
                strategy_id=uuid4(),
                symbol="UNKNOWN",
                broker=BrokerType.ALPACA,
                market_type=MarketType.STOCKS,
                starting_balance=10000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                timeframe=Timeframe.H1,
            )

            with pytest.raises(SymbolNotFoundException):
                mock_markets_service.get_symbol_info = AsyncMock(
                    side_effect=SymbolNotFoundException(request.symbol)
                )
                await backtest_service.create(request, uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_success(
            self,
            backtest_service,
            mock_strategy_service,
            mock_backtest_runner_service,
            mock_markets_service,
        ):
            mock_db_sess = AsyncMock()

            mock_strategy_service.get_user_strategy = AsyncMock()

            mock_result = MagicMock()
            mock_result.first.return_value = True
            mock_db_sess.execute.return_value = mock_result

            mock_db_sess.add = MagicMock()
            mock_db_sess.flush = AsyncMock()
            mock_db_sess.refresh = AsyncMock()

            mock_backtest_runner_service.run_backtest = AsyncMock()

            request = CreateBacktestRequest(
                strategy_id=uuid4(),
                symbol="AAPL",
                broker=BrokerType.ALPACA,
                market_type=MarketType.STOCKS,
                starting_balance=10000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                timeframe=Timeframe.H1,
            )

            mock_markets_service.get_symbol_info = AsyncMock(
                return_value=InstrumentInfo(
                    id=uuid4(),
                    symbol=request.symbol,
                    native_symbol=request.symbol,
                    broker_type=request.broker,
                    market_type=request.market_type,
                    timeframe=request.timeframe,
                    start_date=date(2023, 12, 30),
                    end_date=date(2025, 1, 1),
                )
            )

            result = await backtest_service.create(request, uuid4(), mock_db_sess)

            assert result is not None
            mock_db_sess.add.assert_called_once()
            mock_backtest_runner_service.run.assert_called_once()


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
            mock_backtest.strategy_id = uuid4()
            mock_backtest.symbol = "AAPL"
            mock_backtest.broker = BrokerType.ALPACA
            mock_backtest.market_type = MarketType.STOCKS
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

            mock_instrument = MagicMock()
            mock_instrument.id = uuid4()
            mock_instrument.symbol = "AAPL"
            mock_instrument.native_symbol = "AAPL"
            mock_instrument.broker_type = mock_backtest.broker
            mock_instrument.market_type = mock_backtest.market_type

            mock_db_sess.get.return_value = mock_instrument

            result = await backtest_service.get_backtest(uuid4(), uuid4(), mock_db_sess)

            assert isinstance(result, BacktestResponse)
            assert result.symbol == "AAPL"
            assert result.status == BacktestStatus.COMPLETED


class TestGetBacktests:
    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_backtests_returns_paginated_response(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.id = uuid4()
            mock_backtest.strategy_id = uuid4()
            mock_backtest.symbol = "AAPL"
            mock_backtest.broker = BrokerType.ALPACA
            mock_backtest.market_type = MarketType.STOCKS
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

            mock_instrument = MagicMock()
            mock_instrument.id = uuid4()
            mock_instrument.symbol = "AAPL"
            mock_instrument.native_symbol = "AAPL"
            mock_instrument.broker_type = mock_backtest.broker
            mock_instrument.market_type = mock_backtest.market_type

            mock_result = MagicMock()
            mock_result.all.return_value = [
                (mock_backtest, mock_metrics, mock_instrument)
            ]

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

            with pytest.raises(BacktestInProgressError):
                await backtest_service.delete(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_success(self, backtest_service):
            mock_db_sess = AsyncMock()

            mock_backtest = MagicMock()
            mock_backtest.status = BacktestStatus.COMPLETED

            mock_delete = AsyncMock()
            mock_db_sess.scalar.return_value = mock_backtest
            mock_db_sess.delete = mock_delete

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
                notional=None,
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
        mock_backtest_runner_service,
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
            prompt="test prompt",
            code="class Strategy: pass",
        )
        db_sess.add(strategy)
        await db_sess.flush()

        backtest = Backtest(
            strategy_id=strategy.strategy_id,
            instrument_id=instrument_id,
            starting_balance=10000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            timeframe=Timeframe.H1,
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
        mock_backtest_runner_service,
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
            prompt="test prompt",
            code="class Strategy: pass",
        )
        db_sess.add(strategy)

        other_strategy = Strategy(
            user_id=other_user_id,
            name="Other Strategy",
            description="Other description",
            prompt="other prompt",
            code="class Strategy: pass",
        )
        db_sess.add(other_strategy)

        await db_sess.flush()

        # User-owned backtest
        backtest = Backtest(
            strategy_id=strategy.strategy_id,
            instrument_id=instrument_id,
            starting_balance=10000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            timeframe=Timeframe.H1,
            status=BacktestStatus.COMPLETED,
        )
        db_sess.add(backtest)

        # Other user's backtest
        other_backtest = Backtest(
            strategy_id=other_strategy.strategy_id,
            instrument_id=instrument_id,
            starting_balance=5000,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            timeframe=Timeframe.H1,
            status=BacktestStatus.COMPLETED,
        )
        db_sess.add(other_backtest)

        await db_sess.flush()

        # Order belonging to requesting user
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

        # Order belonging to different user
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

        # Only the requesting user's order should be returned
        assert len(result.data) == 1

        returned_order = result.data[0]

        assert returned_order.id == user_order.id
        assert returned_order.symbol == "AAPL"

        # Ensure another user's order was not returned
        returned_ids = {order.id for order in result.data}
        assert other_user_order.id not in returned_ids
