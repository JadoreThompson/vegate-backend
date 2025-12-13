from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from engine.enums import BrokerType, Timeframe


ActionT = Literal["subscribe", "unsubscribe"]


class SubscribeRequest(BaseModel):
    action: Literal["subscribe"]
    broker: BrokerType
    symbols: list[tuple[str, Timeframe]]


class UnsubscribeRequest(BaseModel):
    action: Literal["unsubscribe"]
    broker: BrokerType
    symbols: list[tuple[str, Timeframe]]

