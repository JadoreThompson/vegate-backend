from sqlalchemy.orm import Session

from core.event import BaseEvent
from core.kafka import KafkaProducer
from .base import SyncEventPublisher
from .util import build_headers


class KafkaSyncEventPublisher(SyncEventPublisher):

    def __init__(self):
        self._kafka_producer: KafkaProducer | None = None
        self._client_healthy = True

    def _get_kafka_producer(self):
        if self._kafka_producer is None or not self._client_healthy:
            self._kafka_producer = KafkaProducer.create()
        return self._kafka_producer

    def publish(self, event: BaseEvent, db_sess: Session | None = None) -> None:
        try:
            kafka_producer = self._get_kafka_producer()
            kafka_producer.send(
                event.topic,
                event.model_dump_json().encode(),
                headers=build_headers(event),
            )
        except Exception:
            self._client_healthy = False
            raise

