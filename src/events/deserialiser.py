from abc import ABC, abstractmethod
from events.base import BaseEvent


class EventDeserialiser(ABC):

    @abstractmethod
    def deserialise(self, event: dict): ...

    @abstractmethod
    def deserialise_json(self, event: str | bytes): ...
