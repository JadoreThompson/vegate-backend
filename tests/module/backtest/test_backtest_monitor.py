import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from config import REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX
from module.backtest.enums import BacktestStatus
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.event.event import (
    BacktestCancelledEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
)
from module.backtest.executor.exception import BacktestLimitReached
from module.notification.enums import NotificationType
from module.notification.schema import BacktestCapacityConstrainedNotificationContext
from core.redis import REDIS_CLIENT
from module.backtest.monitor import BacktestMonitor

MODULE_PATH = "module.backtest.monitor"


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def mock_backtest_executor():
    executor = MagicMock()
    executor.run = AsyncMock()
    executor.stop = AsyncMock()
    return executor


@pytest.fixture
def mock_notification_publisher():
    publisher = MagicMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def mock_kafka_consumer():
    with patch(f"{MODULE_PATH}.AsyncKafkaConsumer") as MockAsyncKafkaConsumer:
        mock_kafka_consumer = MagicMock()
        mock_kafka_consumer.start = AsyncMock()
        mock_kafka_consumer.stop = AsyncMock()
        mock_kafka_consumer.commit = AsyncMock()

        MockAsyncKafkaConsumer.return_value = mock_kafka_consumer

        yield mock_kafka_consumer


@pytest.fixture
def mock_db_sess():
    with patch(f"{MODULE_PATH}.get_db_session") as mock_get_db_session:
        mock_db_sess = MagicMock()
        mock_db_sess.execute = AsyncMock()
        mock_db_sess.commit = AsyncMock()
        mock_db_sess.get = AsyncMock(return_value=MagicMock())

        mock_context_manager = MagicMock()
        mock_context_manager.__aenter__.return_value = mock_db_sess
        mock_context_manager.__aexit__.return_value = None

        mock_get_db_session.return_value = mock_context_manager

        yield mock_db_sess


@pytest.fixture
def deserialiser():
    return BacktestEventDeserialiser()


@pytest.fixture
def backtest_monitor(
    mock_event_publisher,
    mock_backtest_executor,
    mock_notification_publisher,
    mock_kafka_consumer,
    deserialiser,
):
    service = BacktestMonitor(
        deserialiser=deserialiser,
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
        backtest_executor=mock_backtest_executor,
        notification_publisher=mock_notification_publisher,
        monitor_interval=5,
    )

    return service


def create_mock_kafka_record(event):
    mock_record = MagicMock()
    mock_record.headers = [
        ("event_type", BacktestEventType.STATUS_CHANGED.value.encode())
    ]
    mock_record.value = event.model_dump_json().encode()
    return mock_record


class TestHeartbeatMonitoring:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_consume_events_and_watches_backtest(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        """
        Tests a status changed event to in_progress is processed. Adding the backtest
        to its watchlist. After interval with the backtest's heartbeat missing, its status
        is declared suspicious and lastly failed.
        """

        backtest_id = uuid4()

        records = [
            create_mock_kafka_record(
                BacktestStatusChangedEvent(
                    backtest_id=backtest_id, status=BacktestStatus.IN_PROGRESS
                )
            )
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 3 + 5,
            )
        except asyncio.TimeoutError:
            pass

        assert mock_event_publisher.publish.call_count == 2

        args = mock_event_publisher.publish.call_args_list[0][0]
        event = args[0]
        assert event.type == BacktestEventType.STATUS_CHANGED
        assert event.backtest_id == backtest_id
        assert event.status == BacktestStatus.SUSPICIOUS

        args = mock_event_publisher.publish.call_args_list[1][0]
        event = args[0]
        assert event.type == BacktestEventType.STATUS_CHANGED
        assert event.backtest_id == backtest_id
        assert event.status == BacktestStatus.FAILED

        assert mock_db_sess.execute.call_count == 2
        assert (
            mock_db_sess.commit.call_count == 1
        )  # Only runs long enough for status changed to be persisted

    @pytest.mark.asyncio(loop_scope="session")
    async def test_completed_event_is_ignored(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        """
        Tests a status changed event to COMPLETED is ignored and does not
        enqueue any additional events.
        """

        backtest_id = uuid4()

        mock_record = MagicMock()
        mock_record.headers = [
            ("event_type", BacktestEventType.STATUS_CHANGED.value.encode())
        ]

        event = BacktestStatusChangedEvent(
            backtest_id=backtest_id,
            status=BacktestStatus.COMPLETED,
        )

        mock_record.value = event.model_dump_json().encode()

        records = [mock_record]

        try:
            mock_kafka_consumer.__aiter__.return_value = records

            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_event_publisher.publish.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_changes_status_from_suspicious_to_running(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        """
        Tests that after setting a backtest as suspicious and receiving
        a heartbeat its status is set back to in_progress.
        """
        backtest_id = uuid4()

        records = [
            create_mock_kafka_record(
                BacktestStatusChangedEvent(
                    backtest_id=backtest_id, status=BacktestStatus.IN_PROGRESS
                )
            )
        ]

        async def capture_event(event, *args, **kw):
            if (
                event.type == BacktestEventType.STATUS_CHANGED
                and event.status == BacktestStatus.SUSPICIOUS
            ):
                await REDIS_CLIENT.set(
                    f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{event.backtest_id}",
                    1,
                    ex=15,
                )
            pass

        mock_event_publisher.publish.side_effect = capture_event

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        assert mock_event_publisher.publish.call_count == 2

        args = mock_event_publisher.publish.call_args_list[0][0]
        event = args[0]
        assert event.type == BacktestEventType.STATUS_CHANGED
        assert event.backtest_id == backtest_id
        assert event.status == BacktestStatus.SUSPICIOUS

        args = mock_event_publisher.publish.call_args_list[1][0]
        event = args[0]
        assert event.type == BacktestEventType.STATUS_CHANGED
        assert event.backtest_id == backtest_id
        assert event.status == BacktestStatus.IN_PROGRESS

        assert mock_db_sess.execute.call_count == 2
        assert mock_db_sess.commit.call_count == 1

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "terminal_status", [BacktestStatus.COMPLETED, BacktestStatus.FAILED]
    )
    async def test_terminal_status_removes_from_watchlist(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
        terminal_status,
    ):
        """
        Tests that when a backtest reaches a terminal status (COMPLETED, FAILED, CANCELLED),
        it is removed from the watchlist and the monitor loop does not emit heartbeat-based
        status transitions.
        """
        backtest_id = uuid4()

        records = [
            create_mock_kafka_record(
                BacktestStatusChangedEvent(
                    backtest_id=backtest_id, status=BacktestStatus.IN_PROGRESS
                )
            ),
            create_mock_kafka_record(
                BacktestStatusChangedEvent(
                    backtest_id=backtest_id, status=terminal_status
                )
            ),
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_event_publisher.publish.assert_not_called()


class TestBacktestRequestedEvent:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_backtest_not_found(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_backtest_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        mock_db_sess.get.return_value = None

        records = [
            create_mock_kafka_record(BacktestRequestedEvent(backtest_id=backtest_id))
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_backtest_executor.run.assert_not_called()
        mock_event_publisher.publish.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_invalid_status(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_backtest_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        backtest_mock = MagicMock()
        backtest_mock.id = backtest_id
        backtest_mock.status = BacktestStatus.IN_PROGRESS
        mock_db_sess.get.return_value = backtest_mock

        records = [
            create_mock_kafka_record(BacktestRequestedEvent(backtest_id=backtest_id))
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_backtest_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_already_tracked(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_backtest_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        backtest_mock = MagicMock()
        backtest_mock.id = backtest_id
        backtest_mock.status = BacktestStatus.PENDING
        mock_db_sess.get.return_value = backtest_mock

        records = [
            create_mock_kafka_record(
                BacktestStatusChangedEvent(
                    backtest_id=backtest_id, status=BacktestStatus.IN_PROGRESS
                )
            ),
            create_mock_kafka_record(
                BacktestRequestedEvent(backtest_id=backtest_id)
            ),
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_backtest_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_runs_executor(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_backtest_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        backtest_mock = MagicMock()
        backtest_mock.id = backtest_id
        backtest_mock.status = BacktestStatus.PENDING
        mock_db_sess.get.return_value = backtest_mock

        records = [
            create_mock_kafka_record(BacktestRequestedEvent(backtest_id=backtest_id))
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_backtest_executor.run.assert_awaited_once_with(backtest_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_limit_reached(
        self,
        backtest_monitor,
        mock_event_publisher,
        mock_backtest_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        backtest_mock = MagicMock()
        backtest_mock.id = backtest_id
        backtest_mock.status = BacktestStatus.PENDING
        mock_db_sess.get.return_value = backtest_mock

        mock_backtest_executor.run.side_effect = BacktestLimitReached()

        records = [
            create_mock_kafka_record(BacktestRequestedEvent(backtest_id=backtest_id))
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_backtest_executor.run.assert_awaited_once_with(backtest_id)

        mock_event_publisher.publish.assert_called_once()
        args = mock_event_publisher.publish.call_args[0]
        cancelled_event = args[0]
        assert isinstance(cancelled_event, BacktestCancelledEvent)
        assert cancelled_event.backtest_id == backtest_id
        assert cancelled_event.reason == "CAPACITY_CONSTRAINT"


class TestBacktestCancelledEvent:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publishes_notification(
        self,
        backtest_monitor,
        mock_notification_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()
        user_id = uuid4()

        mock_db_sess.scalar = AsyncMock(return_value=user_id)

        records = [
            create_mock_kafka_record(
                BacktestCancelledEvent(
                    backtest_id=backtest_id, reason="CAPACITY_CONSTRAINT"
                )
            )
        ]

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_notification_publisher.publish.assert_called_once_with(
            user_id=user_id,
            type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
            context=BacktestCapacityConstrainedNotificationContext(
                backtest_id=backtest_id
            ),
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_user_not_found_raises_error(
        self,
        backtest_monitor,
        mock_notification_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        backtest_id = uuid4()

        mock_db_sess.scalar = AsyncMock(return_value=None)

        records = [
            create_mock_kafka_record(
                BacktestCancelledEvent(
                    backtest_id=backtest_id, reason="CAPACITY_CONSTRAINT"
                )
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records

        with pytest.raises(Exception, match="User not found for backtest"):
            await asyncio.wait_for(
                backtest_monitor.run(),
                timeout=backtest_monitor.monitor_interval * 2 + 5,
            )

        mock_notification_publisher.publish.assert_not_called()
