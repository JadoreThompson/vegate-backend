from sqlalchemy.ext.asyncio import AsyncSession

from events.base import BaseEvent
from infra.kafka import AsyncKafkaProducer
from .util import build_headers


class EventPublisher:

    def __init__(self):
        self._kafka_producer: AsyncKafkaProducer | None = None
        pass

    async def _get_kafka_producer(self):
        if self._kafka_producer is None:
            self._kafka_producer = AsyncKafkaProducer()
            await self._kafka_producer.start()
        return self._kafka_producer

    async def publish(self, event: BaseEvent) -> None:
        kafka_producer = await self._get_kafka_producer()
        await kafka_producer.send(
            event.topic, event.model_dump_json().encode(), headers=build_headers(event)
        )

    async def enqueue(self, event: BaseEvent, db_sess: AsyncSession | None = None):
        await self.publish(event)
