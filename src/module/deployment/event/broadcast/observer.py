from abc import ABC, abstractmethod
import asyncio
from uuid import UUID

from ..event import DeploymentEventUnion


class DeploymentObserver(ABC):
    """Observer interface — implement this to receive deployment events."""

    @abstractmethod
    async def on_event(
        self, deployment_id: UUID, event: DeploymentEventUnion
    ) -> None: ...


class QueueDeploymentObserver(DeploymentObserver):

    def __init__(self, queue: asyncio.Queue):
        super().__init__()
        self._queue = queue

    async def on_event(self, deployment_id, event):
        self._queue.put_nowait(event)
