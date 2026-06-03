from .base import EventPublisher, SyncEventPublisher


class EventPublisherFactory:

    @staticmethod
    def create(publisher_name: str) -> EventPublisher | SyncEventPublisher:
        if publisher_name == "kafka":
            from .kafka import KakfaEventPublisher
            return KakfaEventPublisher()
        
        elif publisher_name == "sync_kafka":
            from .sync_kafka import KafkaSyncEventPublisher
            return KafkaSyncEventPublisher()
        
        elif publisher_name == "outbox":
            from .outbox import OutboxEventPublisher
            return OutboxEventPublisher()
        
        elif publisher_name == "sync_outbox":
            from .sync_outbox import SyncOutboxEventPublisher
            return SyncOutboxEventPublisher()

        else:
            raise ValueError(f"Unsupported event publisher: {publisher_name}")
