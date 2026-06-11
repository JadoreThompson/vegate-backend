import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from module.backtest.enums import BacktestStatus
from module.backtest.event.event import (
    BacktestCancelledEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
    BacktestStopRequestedEvent,
)
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.executor.exception import BacktestLimitReached
from module.backtest.manager.event_handler import BacktestEventHandler
from module.backtest.manager.state import BacktestState

MODULE_PATH = "module.backtest.manager.event_handler"


def make_db_sess():
    sess = AsyncMock()
    sess.execute = AsyncMock()
    sess.commit = AsyncMock()
    sess.get = AsyncMock(return_value=MagicMock())
    return sess


def make_kafka_record(event):
    record = MagicMock()
    record.headers = [("event_type", event.type.value.encode())]
    record.value = event.model_dump_json().encode()
    return record


def make_kafka_consumer(records: list):
    mock_consumer = MagicMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.commit = AsyncMock()
    mock_consumer.__aiter__.return_value = records
    return mock_consumer


def make_get_db_session():
    mock_db_sess = AsyncMock()
    mock_ctx = MagicMock()
    mock_ctx.__aenter__.return_value = mock_db_sess
    return mock_ctx, mock_db_sess


@pytest.fixture
def state():
    return BacktestState()


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
def deserialiser():
    return BacktestEventDeserialiser()


@pytest.fixture
def event_handler(
    state,
    mock_event_publisher,
    mock_backtest_executor,
    mock_notification_publisher,
    deserialiser,
):
    return BacktestEventHandler(
        state=state,
        deserialiser=deserialiser,
        event_publisher=mock_event_publisher,
        backtest_executor=mock_backtest_executor,
        notification_publisher=mock_notification_publisher,
    )


class TestHandleStatusChanged:

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "status",
        list(BacktestStatus._value2member_map_.keys()),
    )
    async def test_updates_db_status(self, event_handler, status):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        event = BacktestStatusChangedEvent(backtest_id=backtest_id, status=status)

        db_sess = make_db_sess()
        await event_handler._handle_status_changed(event, db_backtest)

        assert db_backtest.status == status

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_updates_db_and_commits(self, event_handler):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        event = BacktestStatusChangedEvent(
            backtest_id=backtest_id, status=BacktestStatus.IN_PROGRESS
        )
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer.create") as mock_kafka_consumer_create:
            mock_consumer = make_kafka_consumer([kafka_record])
            mock_kafka_consumer_create.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_backtest)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        assert db_backtest.status == BacktestStatus.IN_PROGRESS
        mock_db_sess.commit.assert_called_once()


class TestHandleBacktestRequested:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_runs_executor(self, event_handler, mock_backtest_executor):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_requested(event, db_backtest)

        mock_backtest_executor.run.assert_called_once_with(backtest_id)

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "status",
        [
            BacktestStatus.PENDING,
            BacktestStatus.COMPLETED,
            BacktestStatus.FAILED,
            BacktestStatus.CANCELLED,
        ],
    )
    async def test_accepts_valid_statuses(
        self, event_handler, mock_backtest_executor, status
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = status

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_requested(event, db_backtest)

        mock_backtest_executor.run.assert_called_once_with(backtest_id)

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "status",
        [
            BacktestStatus.IN_PROGRESS,
            BacktestStatus.SUSPICIOUS,
        ],
    )
    async def test_disallows_other_statuses(
        self, event_handler, mock_backtest_executor, status
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = status

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_requested(event, db_backtest)

        mock_backtest_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "state_set", ["pending", "running", "suspicious"],
    )
    async def test_already_tracked_does_not_run_executor(
        self, event_handler, mock_backtest_executor, state, state_set
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        if state_set == "pending":
            await state.add_pending(backtest_id)
        elif state_set == "running":
            await state.add_running(backtest_id)
        else:
            await state.add_suspicious(backtest_id)

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_requested(event, db_backtest)

        mock_backtest_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_limit_reached_publishes_cancelled_event(
        self, event_handler, mock_backtest_executor, mock_event_publisher
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        mock_backtest_executor.run.side_effect = BacktestLimitReached()

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_requested(event, db_backtest)

        mock_backtest_executor.run.assert_called_once_with(backtest_id)

        mock_event_publisher.publish.assert_called_once()
        published_event = mock_event_publisher.publish.call_args[0][0]
        assert isinstance(published_event, BacktestCancelledEvent)
        assert published_event.backtest_id == backtest_id
        assert published_event.reason == "CAPACITY_CONSTRAINT"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_runs_executor_and_commits(
        self, event_handler, mock_backtest_executor
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.PENDING

        event = BacktestRequestedEvent(backtest_id=backtest_id)
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer.create") as mock_kafka_consumer_create:
            mock_consumer = make_kafka_consumer([kafka_record])
            mock_kafka_consumer_create.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_backtest)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        mock_backtest_executor.run.assert_called_once_with(backtest_id)
        mock_db_sess.commit.assert_called_once()


class TestHandleBacktestStopRequested:

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "state_set", ["pending", "running", "suspicious"],
    )
    async def test_active_backtest_calls_stop(
        self, event_handler, mock_backtest_executor, state, state_set
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        if state_set == "pending":
            await state.add_pending(backtest_id)
        elif state_set == "running":
            await state.add_running(backtest_id)
        elif state_set == "suspicious":
            await state.add_suspicious(backtest_id)

        event = BacktestStopRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_stop_requested(event, db_backtest)

        mock_backtest_executor.stop.assert_called_once_with(backtest_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_ignores_non_active_backtest(
        self, event_handler, mock_backtest_executor
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        event = BacktestStopRequestedEvent(backtest_id=backtest_id)
        await event_handler._handle_backtest_stop_requested(event, db_backtest)

        mock_backtest_executor.stop.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_stops_executor_and_commits(
        self, event_handler, state, mock_backtest_executor
    ):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        event = BacktestStopRequestedEvent(backtest_id=backtest_id)
        kafka_record = make_kafka_record(event)

        await state.promote_to_running(backtest_id)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer.create") as mock_kafka_consumer_create:
            mock_consumer = make_kafka_consumer([kafka_record])
            mock_kafka_consumer_create.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_backtest)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        mock_backtest_executor.stop.assert_called_once_with(backtest_id)
        mock_db_sess.commit.assert_called_once()


class TestHandleBacktestCancelled:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publishes_notification_and_updates_db(
        self, event_handler, mock_notification_publisher
    ):
        backtest_id = uuid4()
        user_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        event = BacktestCancelledEvent(
            backtest_id=backtest_id, reason="CAPACITY_CONSTRAINT"
        )

        with patch.object(
            event_handler,
            "_get_user_id_for_backtest",
            AsyncMock(return_value=user_id),
        ):
            db_sess = make_db_sess()
            await event_handler._handle_backtest_cancelled(
                event, db_backtest, db_sess
            )

        from module.notification.schema import (
            BacktestCapacityConstrainedNotificationContext,
        )
        from module.notification.enums import NotificationType

        mock_notification_publisher.publish.assert_called_once_with(
            user_id=user_id,
            type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
            context=BacktestCapacityConstrainedNotificationContext(
                backtest_id=backtest_id
            ),
        )
        assert db_backtest.status == BacktestStatus.CANCELLED

    @pytest.mark.asyncio(loop_scope="session")
    async def test_unknown_reason_raises(self, event_handler):
        backtest_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        event = BacktestCancelledEvent(
            backtest_id=backtest_id, reason="CAPACITY_CONSTRAINT"
        )
        event.reason = "unknown_reason"

        db_sess = make_db_sess()
        with pytest.raises(ValueError, match="Unknown cancellation reason"):
            await event_handler._handle_backtest_cancelled(
                event, db_backtest, db_sess
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_publishes_notification_and_commits(
        self, event_handler, mock_notification_publisher
    ):
        backtest_id = uuid4()
        user_id = uuid4()
        db_backtest = MagicMock()
        db_backtest.id = backtest_id
        db_backtest.status = BacktestStatus.IN_PROGRESS

        event = BacktestCancelledEvent(
            backtest_id=backtest_id, reason="CAPACITY_CONSTRAINT"
        )
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer.create") as mock_kafka_consumer_create:
            mock_consumer = make_kafka_consumer([kafka_record])
            mock_kafka_consumer_create.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_backtest)
                mock_db_sess.scalar = AsyncMock(return_value=user_id)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        from module.notification.schema import (
            BacktestCapacityConstrainedNotificationContext,
        )
        from module.notification.enums import NotificationType

        mock_notification_publisher.publish.assert_called_once_with(
            user_id=user_id,
            type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
            context=BacktestCapacityConstrainedNotificationContext(
                backtest_id=backtest_id
            ),
        )
        mock_db_sess.commit.assert_called_once()
