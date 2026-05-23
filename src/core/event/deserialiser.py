from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from .base import BaseEvent

E = TypeVar("E", bound=BaseEvent)


class EventDeserialiser(ABC, Generic[E]):

    @abstractmethod
    def deserialise_json(self, value: str | bytes) -> E: ...

    @abstractmethod
    def deserialise(self, value: dict) -> E: ...
