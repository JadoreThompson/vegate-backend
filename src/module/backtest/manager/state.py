import asyncio
from uuid import UUID


class BacktestState:

    def __init__(self):
        self._pending: set[UUID] = set()
        self._running: set[UUID] = set()
        self._suspicious: set[UUID] = set()
        self._lock = asyncio.Lock()

    async def add_pending(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._pending.add(backtest_id)

    async def add_running(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._running.add(backtest_id)

    async def add_suspicious(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._suspicious.add(backtest_id)

    async def promote_to_running(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(backtest_id)
            self._suspicious.discard(backtest_id)
            self._running.add(backtest_id)

    async def mark_suspicious(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(backtest_id)
            self._running.discard(backtest_id)
            self._suspicious.add(backtest_id)

    async def discard(self, backtest_id: UUID) -> None:
        async with self._lock:
            self._pending.discard(backtest_id)
            self._running.discard(backtest_id)
            self._suspicious.discard(backtest_id)

    async def is_any(self, backtest_id: UUID) -> bool:
        async with self._lock:
            return (
                backtest_id in self._pending
                or backtest_id in self._running
                or backtest_id in self._suspicious
            )

    async def snapshot(self) -> tuple[set[UUID], set[UUID], set[UUID]]:
        async with self._lock:
            return (
                self._pending.copy(),
                self._running.copy(),
                self._suspicious.copy(),
            )
