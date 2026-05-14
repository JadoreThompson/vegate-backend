from collections import defaultdict

from events.base import BaseEvent
from service.event.publisher.sync import SyncEventPublisherService


class BacktestEventPublisherService(SyncEventPublisherService):

    def __init__(self):
        self._events = defaultdict(list)

    def send(self, topic: str, event: BaseEvent) -> None:
        self._events[topic].append(event)
