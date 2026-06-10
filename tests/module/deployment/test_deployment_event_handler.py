import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event.event import (
    DeploymentCancelledEvent,
    DeploymentEventType,
    DeploymentRequestedEvent,
    DeploymentStatusChangedEvent,
    DeploymentStopRequestedEvent,
)
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.executor.exception import DeploymentLimitReached
from module.deployment.manager.event_handler import DeploymentEventHandler
from module.deployment.manager.state import State

MODULE_PATH = "module.deployment.manager.event_handler"


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
    return State()


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.publish = AsyncMock()
    return publisher


@pytest.fixture
def mock_deployment_executor():
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
    return DeploymentEventDeserialiser()


@pytest.fixture
def event_handler(
    state,
    mock_event_publisher,
    mock_deployment_executor,
    mock_notification_publisher,
    deserialiser,
):
    return DeploymentEventHandler(
        state=state,
        deserialiser=deserialiser,
        event_publisher=mock_event_publisher,
        deployment_executor=mock_deployment_executor,
        notification_publisher=mock_notification_publisher,
    )


class TestHandleStatusChanged:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_db_status_to_running(self, event_handler):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.RUNNING,
        )

        db_sess = make_db_sess()
        await event_handler._handle_status_changed(event, db_deployment, db_sess)

        assert db_deployment.status == StrategyDeploymentStatus.RUNNING

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "status",
        list(StrategyDeploymentStatus._value2member_map_.keys()),
    )
    async def test_updates_db_status(self, event_handler, status):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        event = DeploymentStatusChangedEvent(deployment_id=deployment_id, status=status)

        db_sess = make_db_sess()
        await event_handler._handle_status_changed(event, db_deployment, db_sess)

        assert db_deployment.status == status

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_updates_db_and_commits(self, event_handler):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id, status=StrategyDeploymentStatus.RUNNING
        )
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer") as MockConsumer:
            mock_consumer = make_kafka_consumer([kafka_record])
            MockConsumer.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_deployment)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        assert db_deployment.status == StrategyDeploymentStatus.RUNNING
        mock_db_sess.commit.assert_called_once()


class TestHandleDeploymentRequested:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_runs_executor(self, event_handler, mock_deployment_executor):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_requested(event, db_deployment)

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_accepts_stopped_status(
        self, event_handler, mock_deployment_executor
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.STOPPED

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_requested(event, db_deployment)

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "status",
        [
            StrategyDeploymentStatus.RUNNING,
            StrategyDeploymentStatus.SUSPICIOUS,
            StrategyDeploymentStatus.CANCELLED,
            StrategyDeploymentStatus.STOP_REQUESTED,
        ],
    )
    async def test_disallows_other_statuses(
        self, event_handler, mock_deployment_executor, status
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = status

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_requested(event, db_deployment)

        mock_deployment_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "state_set", ["pending", "running", "suspicious"],
    )
    async def test_db_cancelled_with_state_does_not_run_executor(
        self, event_handler, mock_deployment_executor, state, state_set
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.CANCELLED

        if state_set == "pending":
            await state.add_pending(deployment_id)
        elif state_set == "running":
            await state.add_running(deployment_id)
        else:
            await state.add_suspicious(deployment_id)

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_requested(event, db_deployment)

        mock_deployment_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_limit_reached_publishes_cancelled_event(
        self, event_handler, mock_deployment_executor, mock_event_publisher
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        mock_deployment_executor.run.side_effect = DeploymentLimitReached()

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_requested(event, db_deployment)

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

        mock_event_publisher.publish.assert_called_once()
        published_event = mock_event_publisher.publish.call_args[0][0]
        assert isinstance(published_event, DeploymentCancelledEvent)
        assert published_event.deployment_id == deployment_id
        assert published_event.reason == "capacity_constraint"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_runs_executor_and_commits(
        self, event_handler, mock_deployment_executor
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.PENDING

        event = DeploymentRequestedEvent(deployment_id=deployment_id)
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer") as MockConsumer:
            mock_consumer = make_kafka_consumer([kafka_record])
            MockConsumer.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_deployment)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        mock_deployment_executor.run.assert_called_once_with(deployment_id)
        mock_db_sess.commit.assert_called_once()


class TestHandleDeploymentStopRequested:

    @pytest.mark.asyncio(loop_scope="session")
    @pytest.mark.parametrize(
        "state_set", ["pending", "running", "suspicious"],
    )
    async def test_db_cancelled_with_state_calls_stop(
        self, event_handler, mock_deployment_executor, state, state_set
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.CANCELLED

        if state_set == "pending":
            await state.add_pending(deployment_id)
        elif state_set == "running":
            await state.add_running(deployment_id)
        elif state_set == "suspicious":
            await state.add_suspicious(deployment_id)

        event = DeploymentStopRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_stop_requested(event, db_deployment)

        mock_deployment_executor.stop.assert_called_once_with(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_ignores_non_existing_deployment(
        self, event_handler, mock_deployment_executor
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.CANCELLED

        event = DeploymentStopRequestedEvent(deployment_id=deployment_id)
        await event_handler._handle_deployment_stop_requested(event, db_deployment)

        mock_deployment_executor.stop.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_stops_executor_and_commits(
        self, event_handler, state, mock_deployment_executor
    ):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.RUNNING

        event = DeploymentStopRequestedEvent(deployment_id=deployment_id)
        kafka_record = make_kafka_record(event)

        await state.promote_to_running(deployment_id)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer") as MockConsumer:
            mock_consumer = make_kafka_consumer([kafka_record])
            MockConsumer.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_deployment)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        mock_deployment_executor.stop.assert_called_once_with(deployment_id)
        mock_db_sess.commit.assert_called_once()


class TestHandleDeploymentCancelled:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publishes_notification_and_updates_db(
        self, event_handler, mock_notification_publisher
    ):
        deployment_id = uuid4()
        user_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.RUNNING

        event = DeploymentCancelledEvent(
            deployment_id=deployment_id, reason="capacity_constraint"
        )

        with patch.object(
            event_handler,
            "_get_user_id_for_deployment",
            AsyncMock(return_value=user_id),
        ):
            db_sess = make_db_sess()
            await event_handler._handle_deployment_cancelled(
                event, db_deployment, db_sess
            )

        from module.notification.schema import (
            DeploymentCapacityConstrainedNotificationContext,
        )
        from module.notification.enums import NotificationType

        mock_notification_publisher.publish.assert_called_once_with(
            user_id=user_id,
            type=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED,
            context=DeploymentCapacityConstrainedNotificationContext(
                deployment_id=deployment_id
            ),
        )
        assert db_deployment.status == StrategyDeploymentStatus.CANCELLED

    @pytest.mark.asyncio(loop_scope="session")
    async def test_unknown_reason_raises(self, event_handler):
        deployment_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.RUNNING

        event = DeploymentCancelledEvent(
            deployment_id=deployment_id, reason="capacity_constraint"
        )
        event.reason = "unknown_reason"

        db_sess = make_db_sess()
        with pytest.raises(ValueError, match="Unknown cancellation reason"):
            await event_handler._handle_deployment_cancelled(
                event, db_deployment, db_sess
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_through_run_publishes_notification_and_commits(
        self, event_handler, mock_notification_publisher
    ):
        deployment_id = uuid4()
        user_id = uuid4()
        db_deployment = MagicMock()
        db_deployment.deployment_id = deployment_id
        db_deployment.status = StrategyDeploymentStatus.RUNNING

        event = DeploymentCancelledEvent(
            deployment_id=deployment_id, reason="capacity_constraint"
        )
        kafka_record = make_kafka_record(event)

        with patch(f"{MODULE_PATH}.AsyncKafkaConsumer") as MockConsumer:
            mock_consumer = make_kafka_consumer([kafka_record])
            MockConsumer.return_value = mock_consumer

            with patch(f"{MODULE_PATH}.get_db_session") as mock_get_session:
                mock_ctx, mock_db_sess = make_get_db_session()
                mock_db_sess.get = AsyncMock(return_value=db_deployment)
                mock_db_sess.scalar = AsyncMock(return_value=user_id)
                mock_get_session.return_value = mock_ctx

                try:
                    await asyncio.wait_for(event_handler.run(), timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        from module.notification.schema import (
            DeploymentCapacityConstrainedNotificationContext,
        )
        from module.notification.enums import NotificationType

        mock_notification_publisher.publish.assert_called_once_with(
            user_id=user_id,
            type=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED,
            context=DeploymentCapacityConstrainedNotificationContext(
                deployment_id=deployment_id
            ),
        )
        mock_db_sess.commit.assert_called_once()
