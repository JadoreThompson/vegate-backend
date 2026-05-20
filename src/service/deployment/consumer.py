import asyncio
from uuid import UUID

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from events.deployment import DeploymentEventT, DeploymentEventDeserialiser
from infra.kafka.client import AsyncKafkaConsumer, KafkaConsumer


class StrategyDeploymentEventsConsumer:

    def __init__(self):
        self._registry: dict[UUID, asyncio.Queue[DeploymentEventT]] = {}
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
        consumer = AsyncKafkaConsumer(
            STRATEGY_DEPLOYMENT_EVENTS_KEY,
            group_id="event_consumer_group",
            enable_auto_commit=False,
        )
        deserialiser = DeploymentEventDeserialiser()

        # import aiokafka

        # consumer = aiokafka.AIOKafkaConsumer(
        #     STRATEGY_DEPLOYMENT_EVENTS_KEY,
        #     bootstrap_servers="localhost:9092",
        #     group_id="client_relay_group",
        #     enable_auto_commit=False,
        #     # Give heartbeat more breathing room
        #     heartbeat_interval_ms=3000,
        #     session_timeout_ms=30000,
        #     max_poll_interval_ms=300000,
        # )

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
                                deserialiser.deserialise_json(record.value)
                            )

                await consumer.commit()
        finally:
            await consumer.stop()
