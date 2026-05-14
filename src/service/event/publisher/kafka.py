import json

from events.base import BaseEvent
from infra.kafka.client import AsyncKafkaProducer, KafkaProducer
from service.event.publisher.sync import SyncEventPublisherService


# TODO: Implement outbox
class KafkaSyncEventPublisherService(SyncEventPublisherService):

    def __init__(self, kakfa_producer: KafkaProducer):
        self._kafka_producer = kakfa_producer

    def send(self, topic: str, event: BaseEvent):
        self._kafka_producer.send(topic, json.dumps(event.model_dump(mode="json")))
