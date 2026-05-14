from abc import abstractmethod

from events.base import BaseEvent


# TODO: Implement outbox
class SyncEventPublisherService:

    def __init__(self):
        pass

    @abstractmethod
    def send(self, topic: str, event: BaseEvent): ...
