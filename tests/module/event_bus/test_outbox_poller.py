import asyncio
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from core.db import get_db_session, get_db_sess_sync
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event import DeploymentStatusChangedEvent
from module.event_bus.enums import EventStatus
from module.event_bus.model import EventOutbox
from module.event_bus.outbox_poller import OutboxPoller


def make_deployment_event(deployment_id=None, status=None):
    if deployment_id is None:
        deployment_id = uuid4()
    if status is None:
        status = StrategyDeploymentStatus.RUNNING
    return DeploymentStatusChangedEvent(
        deployment_id=deployment_id,
        status=status,
    )


async def insert_outbox_event(
    deployment_id=None,
    status=EventStatus.PENDING,
    type_str="deployment.status",
    extra_pause=0,
):
    event = make_deployment_event(deployment_id=deployment_id)
    raw = event.model_dump(mode="json")

    for _ in range(extra_pause):
        await asyncio.sleep(0.001)

    async with get_db_session() as db_sess:
        outbox = EventOutbox(
            id=event.id,
            type=type_str,
            payload=raw,
            status=status,
            timestamp=event.timestamp,
        )
        db_sess.add(outbox)
        await db_sess.commit()
        return outbox


class TestFetchEvents:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_only_pending_and_failed(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )

        pending = await insert_outbox_event(status=EventStatus.PENDING)
        failed = await insert_outbox_event(status=EventStatus.FAILED)
        completed = await insert_outbox_event(status=EventStatus.COMPLETED)

        records = await poller._fetch_events()

        record_ids = {r.id for r in records}
        assert pending.id in record_ids
        assert failed.id in record_ids
        assert completed.id not in record_ids

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetches_ordered_by_timestamp_asc(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )

        n1 = await insert_outbox_event(extra_pause=1)
        n2 = await insert_outbox_event(extra_pause=1)
        n3 = await insert_outbox_event(extra_pause=1)

        records = await poller._fetch_events()

        assert len(records) >= 3
        idx1 = next(i for i, r in enumerate(records) if r.id == n1.id)
        idx2 = next(i for i, r in enumerate(records) if r.id == n2.id)
        idx3 = next(i for i, r in enumerate(records) if r.id == n3.id)
        assert idx1 < idx2 < idx3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_respects_batch_size(self, mock_kafka_producer):
        for _ in range(5):
            await insert_outbox_event()

        small_poller = OutboxPoller(
            interval=1, batch_size=3, kafka_producer=mock_kafka_producer
        )
        records = await small_poller._fetch_events()
        assert len(records) == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_constructor_args_are_adhered_to(self):
        poller = OutboxPoller(
            interval=10, batch_size=50, kafka_producer=MagicMock(), timeout=15
        )

        assert poller.interval == 10
        assert poller.batch_size == 50
        assert poller.timeout == 15


class TestEmitEvent:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_emits_event_via_kafka(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        event = make_deployment_event()
        raw = event.model_dump(mode="json")

        outbox_id, success = await poller._emit_event(uuid4(), raw)

        assert success is True
        mock_kafka_producer.send_and_wait.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_kafka_error(self, mock_kafka_producer):
        mock_kafka_producer.send_and_wait.side_effect = Exception("kafka down")
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        event = make_deployment_event()
        raw = event.model_dump(mode="json")

        outbox_id, success = await poller._emit_event(uuid4(), raw)

        assert success is False

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_false_on_unparseable_event(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        raw = {"type": "unknown.event", "foo": "bar"}

        outbox_id, success = await poller._emit_event(uuid4(), raw)

        assert success is False
        mock_kafka_producer.send_and_wait.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_returns_event_id(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        event = make_deployment_event()
        raw = event.model_dump(mode="json")
        outbox_id = uuid4()

        returned_id, success = await poller._emit_event(outbox_id, raw)

        assert returned_id == outbox_id
        assert success is True


class TestUpdateEvents:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_updates_statuses(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        n1 = await insert_outbox_event()
        n2 = await insert_outbox_event()
        n3 = await insert_outbox_event()

        await poller._update_events(
            [
                (n1.id, EventStatus.COMPLETED),
                (n2.id, EventStatus.FAILED),
                (n3.id, EventStatus.COMPLETED),
            ]
        )

        async with get_db_session() as db_sess:
            result = await db_sess.execute(
                select(EventOutbox).where(
                    EventOutbox.id.in_([n1.id, n2.id, n3.id])
                )
            )
            records = {r.id: r for r in result.scalars().all()}

        assert records[n1.id].status == EventStatus.COMPLETED.value
        assert records[n2.id].status == EventStatus.FAILED.value
        assert records[n3.id].status == EventStatus.COMPLETED.value

    @pytest.mark.asyncio(loop_scope="session")
    async def test_no_updates_for_empty_list(self, mock_kafka_producer):
        poller = OutboxPoller(
            interval=1, batch_size=100, kafka_producer=mock_kafka_producer
        )
        await poller._update_events([])


class TestRunLifecycle:

    @pytest.fixture(autouse=True)
    def _clear_outbox(self):
        with get_db_sess_sync() as db_sess:
            db_sess.execute(delete(EventOutbox))

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_processes_all_events(self, mock_kafka_producer):
        n1 = await insert_outbox_event()
        n2 = await insert_outbox_event()
        n3 = await insert_outbox_event()

        poller = OutboxPoller(
            interval=0.05, batch_size=100, kafka_producer=mock_kafka_producer
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
                r = await db_sess.get(EventOutbox, record.id)
                assert r.status == EventStatus.COMPLETED.value

        assert mock_kafka_producer.send_and_wait.await_count >= 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_completed_events_are_not_re_fetched(self, mock_kafka_producer):
        n1 = await insert_outbox_event()

        poller = OutboxPoller(
            interval=0.05, batch_size=100, kafka_producer=mock_kafka_producer
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
            r = await db_sess.get(EventOutbox, n1.id)
            assert r.status == EventStatus.COMPLETED.value

        assert mock_kafka_producer.send_and_wait.await_count == 1

        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(EventOutbox).where(
                    EventOutbox.status.in_(
                        [EventStatus.PENDING, EventStatus.FAILED]
                    )
                )
            )
            remaining = res.scalars().all()

        assert len(remaining) == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_failed_events_are_retried(self, mock_kafka_producer):
        n1 = await insert_outbox_event()
        n2 = await insert_outbox_event()

        call_count = 0

        async def flaky_send(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("first attempt fails")
            return None

        mock_kafka_producer.send_and_wait.side_effect = flaky_send

        poller = OutboxPoller(
            interval=0.05, batch_size=100, kafka_producer=mock_kafka_producer
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 4:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()

        async with get_db_session() as db_sess:
            r1 = await db_sess.get(EventOutbox, n1.id)
            r2 = await db_sess.get(EventOutbox, n2.id)

        assert r1.status == EventStatus.COMPLETED.value
        assert r2.status == EventStatus.COMPLETED.value
        assert mock_kafka_producer.send_and_wait.await_count == 3

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persistent_failure_remains_failed(self, mock_kafka_producer):
        n1 = await insert_outbox_event()

        mock_kafka_producer.send_and_wait.side_effect = Exception("always fails")

        poller = OutboxPoller(
            interval=0.05, batch_size=100, kafka_producer=mock_kafka_producer
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
            r = await db_sess.get(EventOutbox, n1.id)

        assert r.status == EventStatus.FAILED.value

    @pytest.mark.asyncio(loop_scope="session")
    async def test_skips_when_no_pending_events(self, mock_kafka_producer):
        async with get_db_session() as db_sess:
            await db_sess.execute(delete(EventOutbox))
            await db_sess.commit()

        poller = OutboxPoller(
            interval=0.05, batch_size=100, kafka_producer=mock_kafka_producer
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

        mock_kafka_producer.send_and_wait.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_respects_batch_size_in_run(self, mock_kafka_producer):
        for _ in range(5):
            await insert_outbox_event()

        poller = OutboxPoller(
            interval=0.05, batch_size=2, kafka_producer=mock_kafka_producer
        )

        original_fetch = poller._fetch_events
        fetch_calls = 0

        async def controlled_fetch():
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls > 5:
                raise asyncio.CancelledError()
            return await original_fetch()

        poller._fetch_events = controlled_fetch

        with pytest.raises(asyncio.CancelledError):
            await poller.run()

        assert mock_kafka_producer.send_and_wait.await_count == 5
