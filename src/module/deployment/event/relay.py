import asyncio
from uuid import UUID

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from core.kafka import AsyncKafkaConsumer
from .deserialiser import DeploymentEventDeserialiser
from .event import DeploymentEventUnion


class DeploymentEventRelay:
    """
    Consumes a stream of deployment events and pushes them to a queue for each
    deployment. Used within the /stream endpoint to relay events through the stream
    """

    def __init__(
        self,
        deserialiser: DeploymentEventDeserialiser,
        topic: str = STRATEGY_DEPLOYMENT_EVENTS_KEY,
    ):
        self._topic = topic
        self._deserialiser = deserialiser
        self._registry: dict[UUID, asyncio.Queue[DeploymentEventUnion]] = {}
        self._lock = asyncio.Lock()
        pass

    async def register(self, deployment_id: UUID):
        async with self._lock:
            if deployment_id in self._registry:
                raise ValueError(f"'{deployment_id} is already registered")

            queue = asyncio.Queue()
            self._registry[deployment_id] = queue
            return queue

    async def remove(self, deployment_id: UUID):
        async with self._lock:
            if deployment_id not in self._registry:
                raise ValueError(f"'{deployment_id} is not registered")
            self._registry.pop(deployment_id)

    async def run(self):
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

                        queue = self._registry.get(deployment_id)

                        if queue is not None:
                            queue.put_nowait(
                                self._deserialiser.deserialise_json(record.value)
                            )

                        break

                await consumer.commit()
        finally:
            await consumer.stop()
