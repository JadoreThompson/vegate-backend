from uuid import uuid4

import pytest
from core.db import get_db_session, get_db_sess_sync
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event import DeploymentEventType, DeploymentStatusChangedEvent
from module.event_bus.enums import EventStatus
from module.event_bus.model import EventOutbox
from module.event_bus.publisher.outbox import OutboxEventPublisher
from module.event_bus.publisher.sync_outbox import SyncOutboxEventPublisher


def make_deployment_event(deployment_id=None, status=None):
    if deployment_id is None:
        deployment_id = uuid4()
    if status is None:
        status = StrategyDeploymentStatus.RUNNING
    return DeploymentStatusChangedEvent(
        deployment_id=deployment_id,
        status=status,
    )


class TestOutboxEventPublisher:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publish_persists_event_with_pending_status(self):
        publisher = OutboxEventPublisher()
        event = make_deployment_event()

        async with get_db_session() as db_sess:
            await publisher.publish(event, db_sess)

        async with get_db_session() as check_sess:
            row = await check_sess.get(EventOutbox, event.id)

        assert row is not None
        assert row.id == event.id
        assert row.type == DeploymentEventType.DEPLOYMENT_STATUS.value
        assert row.payload == event.model_dump(mode="json")
        assert row.status == EventStatus.PENDING.value
        assert row.timestamp == event.timestamp

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publish_without_db_sess_creates_own_session(self):
        publisher = OutboxEventPublisher()
        event = make_deployment_event()

        await publisher.publish(event)

        async with get_db_session() as db_sess:
            row = await db_sess.get(EventOutbox, event.id)

        assert row is not None
        assert row.id == event.id
        assert row.status == EventStatus.PENDING.value


class TestSyncOutboxEventPublisher:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publish_persists_event_with_pending_status(self):
        publisher = SyncOutboxEventPublisher()
        event = make_deployment_event()

        with get_db_sess_sync() as db_sess:
            publisher.publish(event, db_sess)

        async with get_db_session() as check_sess:
            row = await check_sess.get(EventOutbox, event.id)

        assert row is not None
        assert row.id == event.id
        assert row.type == DeploymentEventType.DEPLOYMENT_STATUS.value
        assert row.payload == event.model_dump(mode="json")
        assert row.status == EventStatus.PENDING.value

    @pytest.mark.asyncio(loop_scope="session")
    async def test_publish_without_db_sess_creates_own_session(self):
        publisher = SyncOutboxEventPublisher()
        event = make_deployment_event()

        publisher.publish(event)

        async with get_db_session() as db_sess:
            row = await db_sess.get(EventOutbox, event.id)

        assert row is not None
        assert row.id == event.id
        assert row.status == EventStatus.PENDING.value
