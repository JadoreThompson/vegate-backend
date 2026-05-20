import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from config import REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX
from enums import StrategyDeploymentStatus
from events.deployment import DeploymentEventType, DeploymentStatusChangedEvent
from infra.redis.client import REDIS_CLIENT
from service.monitoring.deployment.service import DeploymentMonitoringService

MODULE_PATH = "service.monitoring.deployment.service"


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

        mock_context_manager = MagicMock()
        mock_context_manager.__aenter__.return_value = mock_db_sess
        mock_context_manager.__aexit__.return_value = None

        mock_get_db_session.return_value = mock_context_manager

        yield mock_db_sess


@pytest.fixture
def deployment_monitoring_service(mock_event_publisher, mock_kafka_consumer):
    service = DeploymentMonitoringService(
        redis_client=REDIS_CLIENT,
        event_publisher=mock_event_publisher,
        monitor_interval=5,
    )

    return service


def create_mock_kafka_record(event):
    mock_record = MagicMock()
    mock_record.headers = [
        ("event_type", DeploymentEventType.DEPLOYMENT_STATUS.value.encode())
    ]
    mock_record.value = event.model_dump_json().encode()
    return mock_record


@pytest.mark.asyncio(loop_scope="session")
async def test_consume_events_and_watches_deployment(
    deployment_monitoring_service,
    mock_event_publisher,
    mock_db_sess,
    mock_kafka_consumer,
):
    """
    Tests a status changed event to status running is processed. Adding the deployment
    to it's watchlist. After interval with the deployment's heartbeat missing, it's status
    is declared suspicious and lastly stopped.
    """

    deployment_id = uuid4()

    records = [
        create_mock_kafka_record(
            DeploymentStatusChangedEvent(
                deployment_id=deployment_id, status=StrategyDeploymentStatus.RUNNING
            )
        )
    ]

    try:
        mock_kafka_consumer.__aiter__.return_value = records
        await asyncio.wait_for(
            deployment_monitoring_service.run(),
            timeout=deployment_monitoring_service.monitor_interval * 3 + 5,
        )
    except asyncio.TimeoutError:
        pass

    assert mock_event_publisher.enqueue.call_count == 2

    args = mock_event_publisher.enqueue.call_args_list[0][0]
    event = args[0]
    assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
    assert event.deployment_id == deployment_id
    assert event.status == StrategyDeploymentStatus.SUSPICIOUS

    args = mock_event_publisher.enqueue.call_args_list[1][0]
    event = args[0]
    assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
    assert event.deployment_id == deployment_id
    assert event.status == StrategyDeploymentStatus.STOPPED

    assert mock_db_sess.execute.call_count == 2
    assert mock_db_sess.commit.call_count >= 2


@pytest.mark.asyncio(loop_scope="session")
async def test_stopped_event_is_ignored(
    deployment_monitoring_service,
    mock_event_publisher,
    mock_db_sess,
    mock_kafka_consumer,
):
    """
    Tests a status changed event to STOPPED is ignored and does not
    enqueue any additional events.
    """

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

    mock_event_publisher.enqueue.assert_not_called()


@pytest.mark.asyncio(loop_scope="session")
async def test_changes_status_from_suspicious_to_running(
    deployment_monitoring_service,
    mock_event_publisher,
    mock_db_sess,
    mock_kafka_consumer,
):
    """
    Tests that after setting a deployment as suspicious and receiving
    a heartbeat it's status is set back to running
    """
    deployment_id = uuid4()

    records = [
        create_mock_kafka_record(
            DeploymentStatusChangedEvent(
                deployment_id=deployment_id, status=StrategyDeploymentStatus.RUNNING
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
        pass

    mock_event_publisher.enqueue.side_effect = capture_event

    try:
        mock_kafka_consumer.__aiter__.return_value = records
        await asyncio.wait_for(
            deployment_monitoring_service.run(),
            timeout=deployment_monitoring_service.monitor_interval * 2 + 5,
        )
    except asyncio.TimeoutError:
        pass

    assert mock_event_publisher.enqueue.call_count == 2

    args = mock_event_publisher.enqueue.call_args_list[0][0]
    event = args[0]
    assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
    assert event.deployment_id == deployment_id
    assert event.status == StrategyDeploymentStatus.SUSPICIOUS

    args = mock_event_publisher.enqueue.call_args_list[1][0]
    event = args[0]
    assert event.type == DeploymentEventType.DEPLOYMENT_STATUS
    assert event.deployment_id == deployment_id
    assert event.status == StrategyDeploymentStatus.RUNNING

    assert mock_db_sess.execute.call_count == 2
    assert mock_db_sess.commit.call_count >= 2
