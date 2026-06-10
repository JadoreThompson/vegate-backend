import asyncio
from uuid import uuid4

import pytest

from module.deployment.manager.state import State


class TestStateAdd:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_pending(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        pending, running, suspicious = await state.snapshot()
        assert id in pending
        assert id not in running
        assert id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_running(self):
        state = State()
        id = uuid4()
        await state.add_running(id)
        pending, running, suspicious = await state.snapshot()
        assert id in running
        assert id not in pending
        assert id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_suspicious(self):
        state = State()
        id = uuid4()
        await state.add_suspicious(id)
        pending, running, suspicious = await state.snapshot()
        assert id in suspicious
        assert id not in pending
        assert id not in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_add_is_idempotent(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        await state.add_pending(id)
        pending, running, suspicious = await state.snapshot()
        assert len(pending) == 1


class TestStateTransitions:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_promote_to_running_from_pending(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        await state.promote_to_running(id)
        pending, running, suspicious = await state.snapshot()
        assert id in running
        assert id not in pending
        assert id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_promote_to_running_from_suspicious(self):
        state = State()
        id = uuid4()
        await state.add_suspicious(id)
        await state.promote_to_running(id)
        pending, running, suspicious = await state.snapshot()
        assert id in running
        assert id not in suspicious
        assert id not in pending

    @pytest.mark.asyncio(loop_scope="session")
    async def test_promote_to_running_not_in_any_set(self):
        state = State()
        id = uuid4()
        await state.promote_to_running(id)
        pending, running, suspicious = await state.snapshot()
        assert id in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_mark_suspicious_from_pending(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        await state.mark_suspicious(id)
        pending, running, suspicious = await state.snapshot()
        assert id in suspicious
        assert id not in pending
        assert id not in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_mark_suspicious_from_running(self):
        state = State()
        id = uuid4()
        await state.add_running(id)
        await state.mark_suspicious(id)
        pending, running, suspicious = await state.snapshot()
        assert id in suspicious
        assert id not in running
        assert id not in pending

    @pytest.mark.asyncio(loop_scope="session")
    async def test_mark_suspicious_not_in_any_set(self):
        state = State()
        id = uuid4()
        await state.mark_suspicious(id)
        pending, running, suspicious = await state.snapshot()
        assert id in suspicious


class TestStateDiscard:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_discard_from_pending(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        await state.discard(id)
        pending, running, suspicious = await state.snapshot()
        assert id not in pending
        assert id not in running
        assert id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_discard_from_running(self):
        state = State()
        id = uuid4()
        await state.add_running(id)
        await state.discard(id)
        pending, running, suspicious = await state.snapshot()
        assert id not in running
        assert id not in pending
        assert id not in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_discard_from_suspicious(self):
        state = State()
        id = uuid4()
        await state.add_suspicious(id)
        await state.discard(id)
        pending, running, suspicious = await state.snapshot()
        assert id not in suspicious
        assert id not in pending
        assert id not in running

    @pytest.mark.asyncio(loop_scope="session")
    async def test_discard_not_in_any_set(self):
        state = State()
        id = uuid4()
        await state.discard(id)
        pending, running, suspicious = await state.snapshot()
        assert id not in pending
        assert id not in running
        assert id not in suspicious


class TestStateIsAny:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_is_any_returns_true_when_in_pending(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)
        assert await state.is_any(id) is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_is_any_returns_true_when_in_running(self):
        state = State()
        id = uuid4()
        await state.add_running(id)
        assert await state.is_any(id) is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_is_any_returns_true_when_in_suspicious(self):
        state = State()
        id = uuid4()
        await state.add_suspicious(id)
        assert await state.is_any(id) is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_is_any_returns_false_when_not_in_any_set(self):
        state = State()
        id = uuid4()
        assert await state.is_any(id) is False


class TestStateSnapshot:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_snapshot_returns_all_sets(self):
        state = State()
        id1, id2, id3 = uuid4(), uuid4(), uuid4()
        await state.add_pending(id1)
        await state.add_running(id2)
        await state.add_suspicious(id3)

        pending, running, suspicious = await state.snapshot()

        assert id1 in pending
        assert id2 in running
        assert id3 in suspicious

    @pytest.mark.asyncio(loop_scope="session")
    async def test_snapshot_isolation(self):
        state = State()
        id = uuid4()
        await state.add_pending(id)

        pending, running, suspicious = await state.snapshot()
        pending.add(uuid4())

        pending_after, _, _ = await state.snapshot()
        assert len(pending_after) == 1


class TestStateConcurrency:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_concurrent_transitions(self):
        state = State()
        id = uuid4()

        async def task1():
            await state.add_pending(id)
            await asyncio.sleep(0.01)
            await state.promote_to_running(id)

        async def task2():
            await state.mark_suspicious(id)

        await asyncio.gather(task1(), task2())

        pending, running, suspicious = await state.snapshot()
        in_any = sum([id in pending, id in running, id in suspicious])
        assert in_any == 1, "ID should be in exactly one set"
