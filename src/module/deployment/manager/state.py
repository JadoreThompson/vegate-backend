import asyncio
from uuid import UUID


class State:

    def __init__(self):
        self._pending: set[UUID] = set()
        self._running: set[UUID] = set()
        self._suspicious: set[UUID] = set()
        self._lock = asyncio.Lock()

    async def add_pending(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._pending.add(deployment_id)

    async def add_running(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._running.add(deployment_id)

    async def add_suspicious(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._suspicious.add(deployment_id)

    async def promote_to_running(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(deployment_id)
            self._suspicious.discard(deployment_id)
            self._running.add(deployment_id)

    async def mark_suspicious(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(deployment_id)
            self._running.discard(deployment_id)
            self._suspicious.add(deployment_id)

    async def discard(self, deployment_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(deployment_id)
            self._running.discard(deployment_id)
            self._suspicious.discard(deployment_id)

    async def is_any(self, deployment_id: UUID) -> bool:
        async with self._lock:
            return (
                deployment_id in self._pending
                or deployment_id in self._running
                or deployment_id in self._suspicious
            )

    async def snapshot(self) -> tuple[set[UUID], set[UUID], set[UUID]]:
        async with self._lock:
            return (
                self._pending.copy(),
                self._running.copy(),
                self._suspicious.copy(),
            )
