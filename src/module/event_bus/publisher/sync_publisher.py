from sqlalchemy.ext.asyncio import AsyncSession

from core.event import BaseEvent
from core.kafka import KafkaProducer
from .util import build_headers


class SyncEventPublisher:

    def __init__(self):
        self._kafka_producer = KafkaProducer()

    def publish(self, event: BaseEvent):
        self._kafka_producer.send(
            event.topic, event.model_dump_json().encode(), headers=build_headers(event)
        )

    def enqueue(self, event: BaseEvent, db_sess: AsyncSession | None = None):
        self.publish(event)
