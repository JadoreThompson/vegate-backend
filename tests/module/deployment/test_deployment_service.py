import pytest
import pytest_asyncio
from datetime import datetime
from uuid import uuid4

from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import delete, insert, select

from module.broker.enums import BrokerType, OrderStatus
from module.broker_connections import BrokerConnectionsService
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.exception import DeploymentNotFoundException
from module.deployment.executor.base import DeploymentExecutor
from module.deployment.model import StrategyDeployments, StrategyDeploymentMetrics
from module.deployment.schema import (
    CreateDeploymentRequest,
    StrategyDeploymentResponse,
    StrategyDeploymentMetricsResponse,
)
from module.deployment.service import DeploymentsService
from module.markets.enums import MarketType, Timeframe
from module.markets.model import Instrument
from module.markets.schema import InstrumentInfo
from module.markets.service import MarketsService
from module.strategy.model import Strategy
from module.broker_connections.model import BrokerConnections
from module.user.model import User
from core.db import get_db_sess_sync, get_db_session
from module.util import create_user, seed_candles


@pytest.fixture
def mock_markets_service():
    return MagicMock(spec=MarketsService)


@pytest.fixture
def mock_deployment_executor():
    service = MagicMock(spec=DeploymentExecutor)
    service.run = AsyncMock()
    service.stop = AsyncMock()
    return service


@pytest.fixture
def mock_broker_connections_service():
    service = MagicMock(spec=BrokerConnectionsService)
    return service


@pytest.fixture
def deployment_service(
    mock_markets_service, mock_deployment_executor, mock_broker_connections_service
):
    return DeploymentsService(
        markets_service=mock_markets_service,
        deployment_executor=mock_deployment_executor,
        broker_connections_service=mock_broker_connections_service,
    )


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(StrategyDeployments))
        db_sess.execute(delete(Strategy))
        db_sess.commit()


@pytest_asyncio.fixture
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


@pytest_asyncio.fixture(scope="module", autouse=True)
def seed():
    seed_candles()


class TestCreateDeployment:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_success_runs_deployment(
            self, deployment_service, mock_markets_service, mock_deployment_executor
        ):
            mock_db_sess = MagicMock()

            mock_info = MagicMock(spec=InstrumentInfo)
            mock_info.id = uuid4()
            mock_markets_service.get_symbol_info = AsyncMock(return_value=mock_info)

            mock_db_sess.flush = AsyncMock()
            mock_db_sess.refresh = AsyncMock(side_effect=lambda obj: None)

            request = CreateDeploymentRequest(
                strategy_id=uuid4(),
                broker_connection_id=uuid4(),
            )

            await deployment_service.create(request, mock_db_sess)

            mock_db_sess.add.assert_called_once()
            mock_deployment_executor.run.assert_awaited_once()


class TestGetDeployment:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_deployment_not_found_raises(self, deployment_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(DeploymentNotFoundException):
                await deployment_service.get(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_deployment_returns_deployment(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_deployment.deployment_id = uuid4()
            mock_db_sess.scalar.return_value = mock_deployment

            result = await deployment_service.get(uuid4(), uuid4(), mock_db_sess)

            assert result == mock_deployment

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_deployment_wrong_user_raises(self, deployment_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(DeploymentNotFoundException):
                await deployment_service.get(uuid4(), uuid4(), mock_db_sess)

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_deployment_other_users_deployment_raises(
            self, deployment_service, db_sess
        ):
            user_a = await create_user("get-deploy-user-a")
            user_b = await create_user("get-deploy-user-b")

            strategy = Strategy(
                user_id=user_a.user_id,
                name="User A Strategy",
                description="Test",
                prompt="test",
                code="class Strategy: pass",
            )
            db_sess.add(strategy)
            await db_sess.flush()

            broker_connection = BrokerConnections(
                user_id=user_b.user_id,
                broker=BrokerType.ALPACA,
                api_key="<api-key>",
                secret_key="<secret-key>",
                broker_account_id="ACC001",
                broker_account_number="1",
            )
            db_sess.add(broker_connection)
            await db_sess.flush()
            await db_sess.refresh(broker_connection)

            deployment = StrategyDeployments(
                strategy_id=strategy.strategy_id,
                broker_connection_id=broker_connection.connection_id,
            )
            db_sess.add(deployment)
            await db_sess.commit()

            async with get_db_session() as new_db_sess:
                with pytest.raises(DeploymentNotFoundException):
                    await deployment_service.get(
                        deployment.deployment_id, user_b.user_id, new_db_sess
                    )


class TestGetAllDeployments:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_all_returns_paginated_response(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_deployment.deployment_id = uuid4()
            mock_deployment.strategy_id = uuid4()
            mock_deployment.broker_connection_id = uuid4()
            mock_deployment.status = StrategyDeploymentStatus.PENDING
            mock_deployment.error_message = None
            mock_deployment.created_at = datetime.now()
            mock_deployment.updated_at = datetime.now()
            mock_deployment.stopped_at = None

            mock_result = MagicMock()
            mock_result.all.return_value = [(mock_deployment, None)]
            mock_db_sess.execute.return_value = mock_result

            result = await deployment_service.get_all(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_all_filters_by_status(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_db_sess.execute.return_value = mock_result

            result = await deployment_service.get_all(
                uuid4(),
                mock_db_sess,
                page=1,
                limit=10,
                status=[StrategyDeploymentStatus.RUNNING],
            )

            assert result.size == 0
            assert result.data == []

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_all_has_next_when_more_results(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_deployments = []
            for _ in range(11):
                d = MagicMock()
                d.deployment_id = uuid4()
                d.strategy_id = uuid4()
                d.broker_connection_id = uuid4()
                d.status = StrategyDeploymentStatus.PENDING
                d.error_message = None
                d.created_at = datetime.now()
                d.updated_at = datetime.now()
                d.stopped_at = None
                mock_deployments.append((d, None))

            mock_result = MagicMock()
            mock_result.all.return_value = mock_deployments
            mock_db_sess.execute.return_value = mock_result

            result = await deployment_service.get_all(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.has_next is True
            assert len(result.data) == 10


class TestStopDeployment:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_stop_deployment_not_found_raises(self, deployment_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(DeploymentNotFoundException):
                await deployment_service.stop(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_stop_already_stopped_does_not_call_runner(
            self, deployment_service, mock_deployment_executor
        ):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_deployment.status = StrategyDeploymentStatus.STOPPED
            mock_db_sess.scalar.return_value = mock_deployment

            await deployment_service.stop(uuid4(), uuid4(), mock_db_sess)

            mock_deployment_executor.stop.assert_not_awaited()

        @pytest.mark.asyncio(loop_scope="session")
        async def test_stop_already_stop_requested_does_not_call_runner(
            self, deployment_service, mock_deployment_executor
        ):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_deployment.status = StrategyDeploymentStatus.STOP_REQUESTED
            mock_db_sess.scalar.return_value = mock_deployment

            await deployment_service.stop(uuid4(), uuid4(), mock_db_sess)

            mock_deployment_executor.stop.assert_not_awaited()

        @pytest.mark.asyncio(loop_scope="session")
        async def test_stop_running_deployment_calls_runner(
            self, deployment_service, mock_deployment_executor
        ):
            mock_db_sess = AsyncMock()

            deployment_id = uuid4()
            mock_deployment = MagicMock()
            mock_deployment.deployment_id = deployment_id
            mock_deployment.status = StrategyDeploymentStatus.RUNNING
            mock_db_sess.scalar.return_value = mock_deployment

            await deployment_service.stop(deployment_id, uuid4(), mock_db_sess)

            mock_deployment_executor.stop.assert_awaited_once_with(deployment_id)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_stop_pending_deployment_calls_runner(
            self, deployment_service, mock_deployment_executor
        ):
            mock_db_sess = AsyncMock()

            deployment_id = uuid4()
            mock_deployment = MagicMock()
            mock_deployment.deployment_id = deployment_id
            mock_deployment.status = StrategyDeploymentStatus.PENDING
            mock_db_sess.scalar.return_value = mock_deployment

            await deployment_service.stop(deployment_id, uuid4(), mock_db_sess)

            mock_deployment_executor.stop.assert_awaited_once_with(deployment_id)


class TestGetOrders:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_orders_not_found_raises(self, deployment_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(DeploymentNotFoundException):
                await deployment_service.get_orders(
                    uuid4(), uuid4(), mock_db_sess, page=1, limit=10
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_orders_returns_paginated_response(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_deployment.deployment_id = uuid4()
            mock_db_sess.scalar.return_value = mock_deployment

            mock_order = MagicMock()
            mock_order.id = uuid4()
            mock_order.broker_order_id = uuid4()
            mock_order.deployment_id = mock_deployment.deployment_id
            mock_order.symbol = "AAPL"
            mock_order.side = "buy"
            mock_order.order_type = "market"
            mock_order.quantity = 10.0
            mock_order.notional = None
            mock_order.filled_quantity = 10.0
            mock_order.limit_price = None
            mock_order.stop_price = None
            mock_order.avg_fill_price = 150.0
            mock_order.status = OrderStatus.FILLED
            mock_order.created_at = datetime.now()
            mock_order.candle_ts = 1000000

            mock_scalars = MagicMock()
            mock_scalars.all.return_value = [mock_order]
            mock_db_sess.scalars.return_value = mock_scalars

            result = await deployment_service.get_orders(
                uuid4(), uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_orders_empty_returns_empty_list(self, deployment_service):
            mock_db_sess = AsyncMock()

            mock_deployment = MagicMock()
            mock_db_sess.scalar.return_value = mock_deployment

            mock_scalars = MagicMock()
            mock_scalars.all.return_value = []
            mock_db_sess.scalars.return_value = mock_scalars

            result = await deployment_service.get_orders(
                uuid4(), uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.size == 0
            assert result.data == []
            assert result.has_next is False
