import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from core.redis import REDIS_CLIENT
from module.backtest.enums import BacktestStatus
from module.backtest.manager.monitor import BacktestMonitor
from module.backtest.manager.state import BacktestState


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def state():
    return BacktestState()


@pytest.fixture
def monitor(state, mock_event_publisher):
    return BacktestMonitor(
        state=state,
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
        monitor_interval=0.01,
    )


@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def clear_redis():
    yield

    await REDIS_CLIENT.flushall()


class TestMonitorLoop:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_pending_no_heartbeat_publishes_suspicious(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_pending(backtest_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == BacktestStatus.SUSPICIOUS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_running_no_heartbeat_publishes_suspicious(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_running(backtest_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == BacktestStatus.SUSPICIOUS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_suspicious_no_heartbeat_publishes_failed(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_suspicious(backtest_id)

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_called_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == BacktestStatus.FAILED
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_pending_with_heartbeat_publishes_in_progress(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_pending(backtest_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{backtest_id}", 1, ex=15
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
        assert event.status == BacktestStatus.IN_PROGRESS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_suspicious_with_heartbeat_publishes_in_progress(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_suspicious(backtest_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{backtest_id}", 1, ex=15
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
        assert event.status == BacktestStatus.IN_PROGRESS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_untracked_heartbeat_publishes_in_progress(
        self, monitor, mock_event_publisher
    ):
        backtest_id = uuid4()
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{backtest_id}", 1, ex=15
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
        assert event.status == BacktestStatus.IN_PROGRESS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_running_with_heartbeat_publishes_in_progress(
        self, monitor, state, mock_event_publisher
    ):
        backtest_id = uuid4()
        await state.add_running(backtest_id)
        await REDIS_CLIENT.set(
            f"{monitor._heartbeat_prefix_key}{backtest_id}", 1, ex=15
        )

        with patch(
            "asyncio.sleep", AsyncMock(side_effect=[None, asyncio.CancelledError()])
        ):
            try:
                await monitor.run()
            except asyncio.CancelledError:
                pass

        mock_event_publisher.publish.assert_awaited_once()
        event = mock_event_publisher.publish.call_args[0][0]
        assert event.status == BacktestStatus.IN_PROGRESS
        assert event.backtest_id == backtest_id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_multiple_backtests_publish_independent_events(
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
        events_by_id = {event.backtest_id: event for event in events}

        assert events_by_id[alive].status == BacktestStatus.IN_PROGRESS
        assert events_by_id[dead].status == BacktestStatus.SUSPICIOUS
