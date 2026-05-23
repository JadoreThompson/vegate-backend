from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from core.event import BaseEvent

T = TypeVar("T", bound=BaseEvent)


class EventDeserialiser(ABC, Generic[T]):

    @abstractmethod
    def deserialise(self, event: dict) -> T: ...

    @abstractmethod
    def deserialise_json(self, event: str | bytes) -> T: ...
