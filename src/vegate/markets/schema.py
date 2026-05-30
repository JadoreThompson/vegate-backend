from vegate.oms.enums import BrokerType
from vegate.core.schema import CustomBaseModel
from .enums import Timeframe, MarketType

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