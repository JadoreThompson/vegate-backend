import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from core.db import get_db_session
from module.notification import Notification as NotificationModel
from module.notification.channel import (
    EmailNotificationChannel,
    NotificationChannel,
    NotificationChannelType,
)
from module.notification.enums import NotificationStatus, NotificationType
from module.notification.poller import NotificationPoller
from module.notification.schema import (
    BacktestCapacityConstrainedNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
    Notification,
)
from module.notification.template.email import EmailNotificationTemplateEngine
from module.util import create_user


@pytest.fixture
def mock_email_service():
    svc = MagicMock()
    svc.send_email = AsyncMock()
    return svc


@pytest.fixture
def email_channel(mock_email_service):
    return EmailNotificationChannel(
        email_service=mock_email_service,
        template_engine=EmailNotificationTemplateEngine(),
    )


@pytest.fixture
def poller(email_channel):
    return NotificationPoller(
        notification_channels={NotificationChannelType.EMAIL: email_channel},
        interval=1,
        batch_size=100,
        timeout=30,
    )


async def insert_notification(
    user_id,
    type_str="deployment.capacity_constrained",
    context=None,
    channel_type="email",
    status=NotificationStatus.PENDING,
):
    if context is None:
        context = {"deployment_id": str(uuid4())}
    async with get_db_session() as db_sess:
        notification = NotificationModel(
            user_id=user_id,
            type=type_str,
            context=context,
            channel_type=channel_type,
            status=status,
        )
        db_sess.add(notification)
        await db_sess.commit()
        return notification


class TestFetchEvents:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_only_pending_and_failed(self, poller):
        user = await create_user("test_fetch_1")
        pending = await insert_notification(user.id, status=NotificationStatus.PENDING)
        failed = await insert_notification(user.id, status=NotificationStatus.FAILED)
        await insert_notification(user.id, status=NotificationStatus.COMPLETED)

        records = await poller._fetch_events()

        record_ids = {r.id for r in records}
        assert pending.id in record_ids
        assert failed.id in record_ids
        assert len(records) == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_ordered_by_created_at_asc(self, poller):
        user = await create_user("test_fetch_2")
        n1 = await insert_notification(user.id)
        n2 = await insert_notification(user.id)
        n3 = await insert_notification(user.id)

        records = await poller._fetch_events()

        assert len(records) >= 3
        indices = {
            n1.id: i for i, r in enumerate(records) for id_ in (r.id,) if id_ == n1.id
        }
        idx1 = next(i for i, r in enumerate(records) if r.id == n1.id)
        idx2 = next(i for i, r in enumerate(records) if r.id == n2.id)
        idx3 = next(i for i, r in enumerate(records) if r.id == n3.id)
        assert idx1 < idx2 < idx3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_respects_batch_size(self):
        user = await create_user("test_fetch_3")
        for _ in range(5):
            await insert_notification(user.id)

        small_poller = NotificationPoller(
            notification_channels={},
            interval=1,
            batch_size=3,
            timeout=30,
        )
        records = await small_poller._fetch_events()
        assert len(records) == 3


class TestEmitNotification:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_sends_email_via_channel(
        self, poller, email_channel, mock_email_service
    ):
        user = await create_user("test_emit_1")
        deployment_id = uuid4()
        record = await insert_notification(
            user.id,
            type_str=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED.value,
            context={"deployment_id": str(deployment_id)},
        )

        event_id, success = await poller._emit_notification(record)

        assert success is True
        assert event_id == record.id
        mock_email_service.send_email.assert_awaited_once()
        call_kwargs = mock_email_service.send_email.await_args.kwargs
        assert call_kwargs["recipient"] == user.email
        assert "Deployment Capacity Constrained" in call_kwargs["subject"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_sends_backtest_email_via_channel(self, poller, mock_email_service):
        user = await create_user("test_emit_2")
        backtest_id = uuid4()
        record = await insert_notification(
            user.id,
            type_str=NotificationType.BACKTEST_CAPACITY_CONSTRAINED.value,
            context={"backtest_id": str(backtest_id)},
        )

        event_id, success = await poller._emit_notification(record)

        assert success is True
        assert event_id == record.id
        mock_email_service.send_email.assert_awaited_once()
        call_kwargs = mock_email_service.send_email.await_args.kwargs
        assert call_kwargs["recipient"] == user.email
        assert "Backtest Capacity Constrained" in call_kwargs["subject"]
        assert str(backtest_id) in call_kwargs["body"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_channel_missing(self, poller):
        user = await create_user("test_emit_3")
        record = await insert_notification(user.id, channel_type="slack")

        event_id, success = await poller._emit_notification(record)

        assert success is False
        assert event_id == record.id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_channel_error(self, poller, mock_email_service):
        user = await create_user("test_emit_4")
        record = await insert_notification(user.id)
        mock_email_service.send_email.side_effect = Exception("API error")

        event_id, success = await poller._emit_notification(record)

        assert success is False
        assert event_id == record.id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_builds_notification_with_correct_context(
        self, poller, email_channel, mock_email_service
    ):
        user = await create_user("test_emit_5")
        deployment_id = uuid4()
        record = await insert_notification(
            user.id,
            type_str=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED.value,
            context={"deployment_id": str(deployment_id)},
        )

        with patch.object(email_channel, "send", wraps=email_channel.send) as spy:
            await poller._emit_notification(record)

            sent_notification: Notification = spy.await_args.args[0]
            assert sent_notification.user_id == user.id
            assert (
                sent_notification.type
                == NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED
            )
            assert isinstance(
                sent_notification.context,
                DeploymentCapacityConstrainedNotificationContext,
            )
            assert sent_notification.context.deployment_id == deployment_id


class TestUpdateEvents:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_statuses(self, poller):
        user = await create_user("test_upd_1")
        n1 = await insert_notification(user.id)
        n2 = await insert_notification(user.id)
        n3 = await insert_notification(user.id)

        await poller._update_events(
            [
                (n1.id, NotificationStatus.COMPLETED),
                (n2.id, NotificationStatus.FAILED),
                (n3.id, NotificationStatus.COMPLETED),
            ]
        )

        async with get_db_session() as db_sess:
            result = await db_sess.execute(
                select(NotificationModel).where(
                    NotificationModel.id.in_([n1.id, n2.id, n3.id])
                )
            )
            records = {r.id: r for r in result.scalars().all()}

        assert records[n1.id].status == NotificationStatus.COMPLETED.value
        assert records[n2.id].status == NotificationStatus.FAILED.value
        assert records[n3.id].status == NotificationStatus.COMPLETED.value

    @pytest.mark.asyncio(loop_scope="session")
    async def test_sets_last_attempted_at(self, poller):
        user = await create_user("test_upd_2")
        n1 = await insert_notification(user.id)

        await poller._update_events([(n1.id, NotificationStatus.COMPLETED)])

        async with get_db_session() as db_sess:
            updated = await db_sess.get(NotificationModel, n1.id)

        assert updated.last_attempted_at is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_no_updates_for_empty_list(self, poller):
        await poller._update_events([])


class TestRunLifecycle:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_processes_all_notifications(
        self, email_channel, mock_email_service
    ):
        user = await create_user("test_lc_1")
        n1 = await insert_notification(user.id)
        n2 = await insert_notification(user.id)
        n3 = await insert_notification(user.id)

        poller = NotificationPoller(
            notification_channels={NotificationChannelType.EMAIL: email_channel},
            interval=0.05,
            batch_size=100,
            timeout=30,
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 3:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()

        async with get_db_session() as db_sess:
            for record in [n1, n2, n3]:
                r = await db_sess.get(NotificationModel, record.id)
                assert r.status == NotificationStatus.COMPLETED.value
                assert r.last_attempted_at is not None

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_respects_batch_size_and_ordering(
        self, email_channel, mock_email_service
    ):
        user = await create_user("test_lc_2")
        n1 = await insert_notification(user.id)
        n2 = await insert_notification(user.id)
        n3 = await insert_notification(user.id)

        sent = []

        async def recording_send(notification):
            sent.append(notification)
            await email_channel.send(notification)

        recording_channel = MagicMock(spec=["send"])
        recording_channel.send = recording_send

        poller = NotificationPoller(
            notification_channels={NotificationChannelType.EMAIL: recording_channel},
            interval=0.05,
            batch_size=2,
            timeout=30,
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 3:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()

        assert len(sent) == 3
        assert str(sent[0].context.deployment_id) == n1.context["deployment_id"]
        assert str(sent[1].context.deployment_id) == n2.context["deployment_id"]
        assert str(sent[2].context.deployment_id) == n3.context["deployment_id"]

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_handles_channel_failure(self, email_channel, mock_email_service):
        user = await create_user("test_lc_3")
        n1 = await insert_notification(user.id)
        n2 = await insert_notification(user.id)

        n1_deployment_id = UUID(n1.context["deployment_id"])

        async def failing_send(notification):
            if notification.context.deployment_id == n1_deployment_id:
                raise Exception("channel error")
            await email_channel.send(notification)

        failing_channel = MagicMock(spec=["send"])
        failing_channel.send = failing_send

        poller = NotificationPoller(
            notification_channels={NotificationChannelType.EMAIL: failing_channel},
            interval=0.05,
            batch_size=100,
            timeout=30,
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 3:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()

        async with get_db_session() as db_sess:
            r1 = await db_sess.get(NotificationModel, n1.id)
            r2 = await db_sess.get(NotificationModel, n2.id)

        assert r1.status == NotificationStatus.FAILED.value
        assert r1.last_attempted_at is not None
        assert r2.status == NotificationStatus.COMPLETED.value
        assert r2.last_attempted_at is not None

        assert mock_email_service.send_email.await_count == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_skips_when_no_notifications(self, email_channel):
        poller = NotificationPoller(
            notification_channels={NotificationChannelType.EMAIL: email_channel},
            interval=0.05,
            batch_size=100,
            timeout=30,
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 2:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()
