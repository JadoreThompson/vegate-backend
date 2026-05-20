from sqlalchemy.ext.asyncio import AsyncSession

from events.base import BaseEvent
from events.deployment import DeploymentEventType
from infra.db.model.event_outbox import EventOutbox
from infra.kafka.client import AsyncKafkaProducer, KafkaProducer


def build_headers(event: BaseEvent):
    headers = [("event_type", event.type.value.encode())]

    if isinstance(event.type, DeploymentEventType):
        headers.append(("deployment_id", str(event.deployment_id).encode()))

    return headers


class EventPublisher:

    def __init__(self):
        self._kafka_producer: AsyncKafkaProducer | None = None
        pass

    async def enqueue(
        self, event: BaseEvent, topic: str, db_sess: AsyncSession | None = None
    ):
        if self._kafka_producer is None:
            self._kafka_producer = AsyncKafkaProducer()
            await self._kafka_producer.start()

        await self._kafka_producer.send(
            topic, event.model_dump_json().encode(), headers=build_headers(event)
        )


class SyncEventPublisher:

    def __init__(self):
        self._kafka_producer = KafkaProducer()

    def enqueue(
        self, event: BaseEvent, topic: str, db_sess: AsyncSession | None = None
    ):
        self._kafka_producer.send(
            topic, event.model_dump_json().encode(), headers=build_headers(event)
        )
