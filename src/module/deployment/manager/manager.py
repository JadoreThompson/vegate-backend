import asyncio

from .event_handler import DeploymentEventHandler
from .monitor import DeploymentMonitor


class DeploymentManager:

    def __init__(
        self, *, event_handler: DeploymentEventHandler, monitor: DeploymentMonitor
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
            return_exceptions=True,
        )
        exceptions = [r for r in results if isinstance(r, BaseException)]
        if exceptions:
            raise ExceptionGroup("DeploymentManager failed", exceptions)
