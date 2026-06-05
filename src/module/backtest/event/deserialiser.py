import json
from typing import Type

from core.protocol import EventDeserialiser
from .event import (
    BacktestCancelledEvent,
    BacktestEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
    BacktestStopRequestedEvent,
)


class BacktestEventDeserialiser(EventDeserialiser[BacktestEvent]):

    def __init__(self):
        self._registry: dict[BacktestEventType, Type[BacktestEvent]] = {
            BacktestEventType.STATUS_CHANGED: BacktestStatusChangedEvent,
            BacktestEventType.REQUESTED: BacktestRequestedEvent,
            BacktestEventType.STOP_REQUESTED: BacktestStopRequestedEvent,
            BacktestEventType.CANCELLED: BacktestCancelledEvent,
        }

    def deserialise_json(self, payload: str | bytes):
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        data = json.loads(payload)
        return self.deserialise(data)

    def deserialise(self, data: dict):
        try:
            event_type = BacktestEventType(data["type"])
        except KeyError:
            raise ValueError("Missing event type field")
        except ValueError:
            raise ValueError(f"Unknown event type '{data['type']}'")

        model = self._registry[event_type]

        return model.model_validate(data)
