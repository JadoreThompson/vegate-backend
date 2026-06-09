import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from config import REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX
from core.redis import REDIS_CLIENT
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.event.event import (
    DeploymentCancelledEvent,
    DeploymentEventType,
    DeploymentRequestedEvent,
    DeploymentStatusChangedEvent,
)
from module.deployment.event.listener import DeploymentEventListenerService
from module.deployment.executor.exception import DeploymentLimitReached

MODULE_PATH = "module.deployment.event.listener"


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
        mock_db_sess = AsyncMock()
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
    return DeploymentEventDeserialiser()


@pytest.fixture
def deployment_monitoring_service(
    mock_event_publisher,
    mock_deployment_executor,
    mock_notification_publisher,
    mock_kafka_consumer,
    deserialiser,
):
    service = DeploymentEventListenerService(
        deserialiser=deserialiser,
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
        deployment_executor=mock_deployment_executor,
        notification_publisher=mock_notification_publisher,
        monitor_interval=5,
    )

    return service


def create_mock_kafka_record(event):
    mock_record = MagicMock()
    mock_record.headers = [("event_type", event.type.value.encode())]
    mock_record.value = event.model_dump_json().encode()
    return mock_record


class TestMonitorLifecycle:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_consume_events_and_watches_deployment(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        queue = asyncio.Queue()

        mock_event_publisher.publish

        async def _get_records():
            nonlocal queue

            queue.put_nowait(
                create_mock_kafka_record(
                    DeploymentStatusChangedEvent(
                        deployment_id=deployment_id,
                        status=StrategyDeploymentStatus.RUNNING,
                    )
                )
            )

            while True:
                yield await queue.get()

        async def publish_side_effect(event):
            record = create_mock_kafka_record(event)
            await queue.put(record)

        mock_event_publisher.publish = AsyncMock(side_effect=publish_side_effect)

        try:
            mock_kafka_consumer.__aiter__.side_effect = _get_records
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval * 3 + 5,
            )
        except asyncio.TimeoutError:
            pass

        assert mock_event_publisher.publish.call_count == 2

        args = mock_event_publisher.publish.call_args_list[0][0]
        event = args[0]
        assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert event.deployment_id == deployment_id
        assert event.status == StrategyDeploymentStatus.SUSPICIOUS

        args = mock_event_publisher.publish.call_args_list[1][0]
        event = args[0]
        assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert event.deployment_id == deployment_id
        assert event.status == StrategyDeploymentStatus.STOPPED

        assert mock_db_sess.execute.call_count == 3
        assert mock_db_sess.commit.call_count == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stopped_event_is_ignored(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()

        mock_record = MagicMock()
        mock_record.headers = [
            ("event_type", DeploymentEventType.DEPLOYMENT_STATUS.value.encode())
        ]

        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.STOPPED,
        )

        mock_record.value = event.model_dump_json().encode()

        records = [mock_record]

        try:
            mock_kafka_consumer.__aiter__.return_value = records

            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_event_publisher.publish.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_changes_status_from_suspicious_to_running(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()

        records = [
            create_mock_kafka_record(
                DeploymentStatusChangedEvent(
                    deployment_id=deployment_id,
                    status=StrategyDeploymentStatus.RUNNING,
                )
            )
        ]

        async def capture_event(event, *args, **kw):
            if (
                event.type == DeploymentEventType.DEPLOYMENT_STATUS
                and event.status == StrategyDeploymentStatus.SUSPICIOUS
            ):
                await REDIS_CLIENT.set(
                    f"{REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX}{event.deployment_id}",
                    1,
                    ex=15,
                )

        mock_event_publisher.publish.side_effect = capture_event

        try:
            mock_kafka_consumer.__aiter__.return_value = records
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval * 2 + 5,
            )
        except asyncio.TimeoutError:
            pass

        assert mock_event_publisher.publish.call_count == 2

        args = mock_event_publisher.publish.call_args_list[0][0]
        event = args[0]
        assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert event.deployment_id == deployment_id
        assert event.status == StrategyDeploymentStatus.SUSPICIOUS

        args = mock_event_publisher.publish.call_args_list[1][0]
        event = args[0]
        assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert event.deployment_id == deployment_id
        assert event.status == StrategyDeploymentStatus.RUNNING

        assert mock_db_sess.execute.call_count == 1
        assert mock_db_sess.commit.call_count == 1


class TestHandleDeploymentRequested:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_deployment_requested_runs_executor(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = StrategyDeploymentStatus.PENDING
        mock_db_sess.get = AsyncMock(return_value=mock_deployment)

        records = [
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_deployment_requested_accepts_stopped_status(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = StrategyDeploymentStatus.STOPPED
        mock_db_sess.get = AsyncMock(return_value=mock_deployment)

        records = [
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_deployment_requested_deployment_not_found(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        mock_db_sess.get = AsyncMock(return_value=None)

        records = [
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_not_called()

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
    async def test_deployment_requested_wrong_status(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
        status,
    ):
        deployment_id = uuid4()
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = status
        mock_db_sess.get = AsyncMock(return_value=mock_deployment)

        records = [
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_deployment_requested_already_in_set(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = StrategyDeploymentStatus.PENDING
        mock_db_sess.get = AsyncMock(return_value=mock_deployment)

        records = [
            create_mock_kafka_record(
                DeploymentStatusChangedEvent(
                    deployment_id=deployment_id,
                    status=StrategyDeploymentStatus.RUNNING,
                )
            ),
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            ),
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_not_called()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_deployment_requested_limit_reached(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_deployment_executor,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = StrategyDeploymentStatus.PENDING
        mock_db_sess.get = AsyncMock(return_value=mock_deployment)

        mock_deployment_executor.run.side_effect = DeploymentLimitReached()

        records = [
            create_mock_kafka_record(
                DeploymentRequestedEvent(deployment_id=deployment_id)
            )
        ]

        mock_kafka_consumer.__aiter__.return_value = records
        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        mock_deployment_executor.run.assert_called_once_with(deployment_id)

        cancel_event_found = False
        for call_args in mock_event_publisher.publish.call_args_list:
            event = call_args[0][0]
            if (
                event.type == DeploymentEventType.DEPLOYMENT_CANCELLED
                and event.reason == "capacity_constraint"
                and event.deployment_id == deployment_id
            ):
                cancel_event_found = True
                break
        assert (
            cancel_event_found
        ), "Expected DeploymentCancelledEvent with capacity_constraint"


class TestMonitorHeartbeatTracking:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_tracks_unknown_heartbeat(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()

        await REDIS_CLIENT.set(
            f"{REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX}{deployment_id}",
            1,
            ex=15,
        )

        mock_kafka_consumer.__aiter__.return_value = []

        try:
            await asyncio.wait_for(
                deployment_monitoring_service.run(),
                timeout=deployment_monitoring_service.monitor_interval + 5,
            )
        except asyncio.TimeoutError:
            pass

        found = False
        for call_args in mock_event_publisher.publish.call_args_list:
            event = call_args[0][0]
            if (
                event.type == DeploymentEventType.DEPLOYMENT_STATUS
                and event.status == StrategyDeploymentStatus.RUNNING
                and event.deployment_id == deployment_id
            ):
                found = True
                break
        assert found, (
            "Expected RUNNING event for deployment with heartbeat "
            "but no prior local cache entry"
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_tracks_unknown_heartbeat_patched_scan(
        self,
        deployment_monitoring_service,
        mock_event_publisher,
        mock_db_sess,
        mock_kafka_consumer,
    ):
        deployment_id = uuid4()
        expected_key = (
            f"{REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX}{deployment_id}"
        )

        async def mock_scan(*args, **kwargs):
            yield expected_key.encode()

        with patch.object(
            deployment_monitoring_service._redis_client, "scan_iter"
        ) as mock_scan_iter:
            mock_scan_iter.side_effect = mock_scan

            mock_kafka_consumer.__aiter__.return_value = []

            try:
                await asyncio.wait_for(
                    deployment_monitoring_service.run(),
                    timeout=deployment_monitoring_service.monitor_interval + 5,
                )
            except asyncio.TimeoutError:
                pass

        found = False
        for call_args in mock_event_publisher.publish.call_args_list:
            event = call_args[0][0]
            if (
                event.type == DeploymentEventType.DEPLOYMENT_STATUS
                and event.status == StrategyDeploymentStatus.RUNNING
                and event.deployment_id == deployment_id
            ):
                found = True
                break
        assert found, (
            "Expected RUNNING event for deployment with patched scan_iter "
            "heartbeat but no prior local cache entry"
        )
