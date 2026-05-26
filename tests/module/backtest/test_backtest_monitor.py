import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from config import REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX
from module.backtest.enums import BacktestStatus
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.event.event import BacktestEventType, BacktestStatusChangedEvent
from core.redis import REDIS_CLIENT
from module.backtest.monitor import BacktestMonitor

MODULE_PATH = "module.backtest.monitor"


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.enqueue = AsyncMock()
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
    mock_event_publisher, mock_kafka_consumer, deserialiser
):
    service = BacktestMonitor(
        deserialiser=deserialiser,
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
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


@pytest.mark.asyncio(loop_scope="session")
async def test_consume_events_and_watches_backtest(
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

    assert mock_event_publisher.enqueue.call_count == 2

    args = mock_event_publisher.enqueue.call_args_list[0][0]
    event = args[0]
    assert event.type == BacktestEventType.STATUS_CHANGED
    assert event.backtest_id == backtest_id
    assert event.status == BacktestStatus.SUSPICIOUS

    args = mock_event_publisher.enqueue.call_args_list[1][0]
    event = args[0]
    assert event.type == BacktestEventType.STATUS_CHANGED
    assert event.backtest_id == backtest_id
    assert event.status == BacktestStatus.FAILED

    assert mock_db_sess.execute.call_count == 2
    assert mock_db_sess.commit.call_count == 1 # Only runs long enough for status changed to be persisted


@pytest.mark.asyncio(loop_scope="session")
async def test_completed_event_is_ignored(
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

    mock_event_publisher.enqueue.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_status_from_suspicious_to_running(
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

    mock_event_publisher.enqueue.side_effect = capture_event

    try:
        mock_kafka_consumer.__aiter__.return_value = records
        await asyncio.wait_for(
            backtest_monitor.run(),
            timeout=backtest_monitor.monitor_interval * 2 + 5,
        )
    except asyncio.TimeoutError:
        pass

    assert mock_event_publisher.enqueue.call_count == 2

    args = mock_event_publisher.enqueue.call_args_list[0][0]
    event = args[0]
    assert event.type == BacktestEventType.STATUS_CHANGED
    assert event.backtest_id == backtest_id
    assert event.status == BacktestStatus.SUSPICIOUS

    args = mock_event_publisher.enqueue.call_args_list[1][0]
    event = args[0]
    assert event.type == BacktestEventType.STATUS_CHANGED
    assert event.backtest_id == backtest_id
    assert event.status == BacktestStatus.IN_PROGRESS

    assert mock_db_sess.execute.call_count == 2
    assert mock_db_sess.commit.call_count == 1


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("terminal_status", [
    BacktestStatus.COMPLETED,
    BacktestStatus.FAILED,
    BacktestStatus.CANCELLED,
])
async def test_terminal_status_removes_from_watchlist(
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

    mock_event_publisher.enqueue.assert_not_called()
