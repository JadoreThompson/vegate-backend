import asyncio

from .event_handler import BacktestEventHandler
from .monitor import BacktestMonitor


class BacktestManager:

    def __init__(
        self, *, event_handler: BacktestEventHandler, monitor: BacktestMonitor
    ):
        self._event_handler = event_handler
        self._monitor = monitor

    def setup(self) -> None:
        self._monitor.setup()

    async def stop(self) -> None:
        await self._event_handler.stop()

    async def run(self) -> None:
        results = await asyncio.gather(
            self._event_handler.run(),
            self._monitor.run(),
        )
        exceptions = [r for r in results if isinstance(r, BaseException)]
        if exceptions:
            raise ExceptionGroup("BacktestManager failed", exceptions)
