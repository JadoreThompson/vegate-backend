import pytest
from module.event_bus.publisher.base import EventPublisher, SyncEventPublisher
from module.event_bus.publisher.factory import EventPublisherFactory
from module.event_bus.publisher.kafka import KakfaEventPublisher
from module.event_bus.publisher.outbox import OutboxEventPublisher
from module.event_bus.publisher.sync_kafka import KafkaSyncEventPublisher
from module.event_bus.publisher.sync_outbox import SyncOutboxEventPublisher


class TestEventPublisherFactory:

    def test_create_kafka_returns_kafka_publisher(self):
        publisher = EventPublisherFactory.create("kafka")
        assert isinstance(publisher, KakfaEventPublisher)
        assert isinstance(publisher, EventPublisher)

    def test_create_sync_kafka_returns_sync_kafka_publisher(self):
        publisher = EventPublisherFactory.create("sync_kafka")
        assert isinstance(publisher, KafkaSyncEventPublisher)
        assert isinstance(publisher, SyncEventPublisher)

    def test_create_outbox_returns_outbox_publisher(self):
        publisher = EventPublisherFactory.create("outbox")
        assert isinstance(publisher, OutboxEventPublisher)
        assert isinstance(publisher, EventPublisher)

    def test_create_sync_outbox_returns_sync_outbox_publisher(self):
        publisher = EventPublisherFactory.create("sync_outbox")
        assert isinstance(publisher, SyncOutboxEventPublisher)
        assert isinstance(publisher, SyncEventPublisher)

    def test_create_unknown_name_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported event publisher: unknown"):
            EventPublisherFactory.create("unknown")
