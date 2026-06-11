import asyncio
from uuid import UUID

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from core.kafka import AsyncKafkaConsumer
from .observer import DeploymentObserver
from ..deserialiser import DeploymentEventDeserialiser
from ..event import DeploymentEventUnion


class DeploymentEventBroadcast:
    """
    Consumes a stream of deployment events and notifies all registered
    observers for each deployment.
    """

    def __init__(
        self,
        deserialiser: DeploymentEventDeserialiser,
        topic: str = STRATEGY_DEPLOYMENT_EVENTS_KEY,
    ):
        self._topic = topic
        self._deserialiser = deserialiser
        self._registry: dict[UUID, list[DeploymentObserver]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self, deployment_id: UUID, observer: DeploymentObserver
    ) -> None:
        async with self._lock:
            self._registry.setdefault(deployment_id, []).append(observer)

    async def unsubscribe(
        self, deployment_id: UUID, observer: DeploymentObserver
    ) -> None:
        async with self._lock:
            observers = self._registry.get(deployment_id)
            if not observers:
                raise ValueError(f"'{deployment_id}' has no registered observers")

            observers.remove(observer)

            if not observers:
                self._registry.pop(deployment_id)

    async def _notify(self, deployment_id: UUID, event: DeploymentEventUnion) -> None:
        async with self._lock:
            observers = list(self._registry.get(deployment_id, []))

        for observer in observers:
            await observer.on_event(deployment_id, event)

    async def run(self) -> None:
        consumer = AsyncKafkaConsumer.create(
            self._topic, group_id="event_consumer_group", enable_auto_commit=False
        )

        try:
            await consumer.start()
            async for record in consumer:
                for key, value in record.headers:
                    if key == "deployment_id":
                        try:
                            deployment_id = UUID(value.decode())
                        except ValueError:
                            break

                        event = self._deserialiser.deserialise_json(record.value)
                        await self._notify(deployment_id, event)
                        break

                await consumer.commit()
        finally:
            await consumer.stop()
