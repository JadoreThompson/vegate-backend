from datetime import datetime
from uuid import UUID

from core.schema import CustomBaseModel
from module.broker.enums import BrokerType
from .enums import MarketType, Timeframe


class InstrumentInfo(CustomBaseModel):
    id: UUID
    symbol: str
    broker_type: BrokerType
    market_type: MarketType
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime


class OHLC(CustomBaseModel):
    """Represents an OHLC candle."""

    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: int
    timeframe: Timeframe
    symbol: str
    broker: BrokerType
    market_type: MarketType
