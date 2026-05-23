from sqlalchemy.ext.asyncio import AsyncSession

from core.event import BaseEvent
from core.kafka import AsyncKafkaProducer
from .util import build_headers


class EventPublisher:

    def __init__(self):
        self._kafka_producer: AsyncKafkaProducer | None = None
        self._client_healthy = True

    async def _get_kafka_producer(self):
        if self._kafka_producer is None or not self._client_healthy:
            self._kafka_producer = AsyncKafkaProducer()
            await self._kafka_producer.start()
        return self._kafka_producer

    async def publish(self, event: BaseEvent) -> None:
        try:
            kafka_producer = await self._get_kafka_producer()
            await kafka_producer.send(
                event.topic, event.model_dump_json().encode(), headers=build_headers(event)
            )
        except Exception:
            self._client_healthy = False
            raise

    async def enqueue(self, event: BaseEvent, db_sess: AsyncSession | None = None):
        await self.publish(event)
