import json

from events.base import BaseEvent
from infra.kafka.client import AsyncKafkaProducer, KafkaProducer


# TODO: Implement outbox
class EventPublisherService:

    def __init__(
        self,
        kakfa_producer: KafkaProducer | None = None,
        async_kakfa_producer: AsyncKafkaProducer | None = None,
    ):
        self._kafka_producer = kakfa_producer
        self._async_kafka_producer = async_kakfa_producer

    def send(self, topic: str, event: BaseEvent):
        self._kafka_producer.send(topic, json.dumps(event.model_dump(mode="json")))

    async def send_async(self, topic: str, event: BaseEvent):
        await self._async_kafka_producer.send(
            topic, json.dumps(event.model_dump(mode="json"))
        )
