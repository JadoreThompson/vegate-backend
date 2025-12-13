from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel

from engine.enums import Timeframe


class CustomBaseModel(BaseModel):
    model_config = {
        "json_encoders": {
            UUID: str,
            datetime: lambda dt: dt.isoformat(),
            Enum: lambda e: e.value,
        }
    }


class OHLCV(CustomBaseModel):
    symbol: str
    timestamp: datetime
    timeframe: Timeframe
    open: float
    high: float
    low: float
    close: float
    volume: float
