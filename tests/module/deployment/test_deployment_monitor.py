import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from core.redis import REDIS_CLIENT
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.manager.monitor import DeploymentMonitor
from module.deployment.manager.state import State


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def state():
    return State()


@pytest.fixture
def monitor(state, mock_event_publisher):
    return DeploymentMonitor(
        state=state,
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
        monitor_interval=0.01,
    )


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clear_redis():
    yield

    await REDIS_CLIENT.flushall()


class TestSetup:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_loads_running_from_db(self, state):
        deployment_id = uuid4()

        with patch(
            "module.deployment.manager.monitor.get_db_sess_sync"
        ) as mock_get_sync:
            mock_sess = MagicMock()
            mock_result = MagicMock()
            mock_result.all.return_value = [
                (deployment_id, StrategyDeploymentStatus.RUNNING),
            ]
            mock_sess.execute.return_value = mock_result
            mock_ctx = MagicMock()
            mock_ctx.__enter__.return_value = mock_sess
            mock_get_sync.return_value = mock_ctx

            monitor = DeploymentMonitor(
                state=state,
                redis_client=MagicMock(),
                event_publisher=MagicMock(),
                monitor_interval=0.01,
            )
            monitor.setup()

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_loads_suspicious_from_db(self, state):
        deployment_id = uuid4()

        with patch(
            "module.deployment.manager.monitor.get_db_sess_sync"
        ) as mock_get_sync:
            mock_sess = MagicMock()
            mock_result = MagicMock()
            mock_result.all.return_value = [
                (deployment_id, StrategyDeploymentStatus.SUSPICIOUS),
            ]
            mock_sess.execute.return_value = mock_result
            mock_ctx = MagicMock()
            mock_ctx.__enter__.return_value = mock_sess
            mock_get_sync.return_value = mock_ctx

            monitor = DeploymentMonitor(
                state=state,
                redis_client=MagicMock(),
                event_publisher=MagicMock(),
                monitor_interval=0.01,
            )
            monitor.setup()

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_ignores_other_statuses(self, state):
        with patch(
            "module.deployment.manager.monitor.get_db_sess_sync"
        ) as mock_get_sync:
            mock_sess = MagicMock()
            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_sess.execute.return_value = mock_result
            mock_ctx = MagicMock()
            mock_ctx.__enter__.return_value = mock_sess
            mock_get_sync.return_value = mock_ctx

            monitor = DeploymentMonitor(
                state=state,
                redis_client=MagicMock(),
                event_publisher=MagicMock(),
                monitor_interval=0.01,
            )
            monitor.setup()

        pending, running, suspicious = await state.snapshot()
        assert len(pending) == 0
        assert len(running) == 0
        assert len(suspicious) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_directly_mutates_state_sets(self, state):
        """setup() accesses state internals directly for synchronous bulk loading."""
        deployment_id = uuid4()
        with patch(
            "module.deployment.manager.monitor.get_db_sess_sync"
        ) as mock_get_sync:
            mock_sess = MagicMock()
            mock_result = MagicMock()
            mock_result.all.return_value = [
                (deployment_id, StrategyDeploymentStatus.RUNNING),
            ]
            mock_sess.execute.return_value = mock_result
            mock_ctx = MagicMock()
            mock_ctx.__enter__.return_value = mock_sess
            mock_get_sync.return_value = mock_ctx

            monitor = DeploymentMonitor(
                state=state,
                redis_client=MagicMock(),
                event_publisher=MagicMock(),
                monitor_interval=0.01,
            )
            monitor.setup()

        async with state._lock:
            assert deployment_id in state._running


class TestRun:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_pending_no_heartbeat_becomes_suspicious(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_pending(deployment_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.SUSPICIOUS
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_running_no_heartbeat_becomes_suspicious(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_running(deployment_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.SUSPICIOUS
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_suspicious_no_heartbeat_becomes_stopped(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_suspicious(deployment_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.STOPPED
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id not in running
        assert deployment_id not in pending
        assert deployment_id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_pending_with_heartbeat_becomes_running(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_pending(deployment_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{deployment_id}", 1, ex=15
        )

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.RUNNING
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_suspicious_with_heartbeat_becomes_running(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_suspicious(deployment_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{deployment_id}", 1, ex=15
        )

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.RUNNING
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_untracked_heartbeat_becomes_running(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{deployment_id}", 1, ex=15
        )

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == StrategyDeploymentStatus.RUNNING
        assert event.deployment_id == deployment_id

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publishes_events_when_no_transitions(
        self, monitor, state, mock_event_publisher
    ):
        deployment_id = uuid4()
        await state.add_running(deployment_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{deployment_id}", 1, ex=15
        )

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_awaited_once()

        pending, running, suspicious = await state.snapshot()
        assert deployment_id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_multiple_deployments_transition_independently(
        self, monitor, state, mock_event_publisher
    ):
        alive = uuid4()
        dead = uuid4()

        await state.add_running(alive)
        await state.add_pending(dead)

        await REDIS_CLIENT.set(f"{monitor._heartbeat_prefix_key}{alive}", 1, ex=15)

        with patch(
            "asyncio.sleep",
            AsyncMock(side_effect=[None, asyncio.CancelledError()]),
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        assert mock_event_publisher.publish.call_count == 2

        events = [call.args[0] for call in mock_event_publisher.publish.call_args_list]

        events_by_id = {event.deployment_id: event for event in events}

        assert events_by_id[alive].status == StrategyDeploymentStatus.RUNNING

        assert events_by_id[dead].status == StrategyDeploymentStatus.SUSPICIOUS

        pending, running, suspicious = await state.snapshot()

        assert alive in running
        assert dead in suspicious
