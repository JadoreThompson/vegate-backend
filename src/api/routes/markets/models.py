from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from core.models import CustomBaseModel
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


class OHLCV(CustomBaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: Timeframe