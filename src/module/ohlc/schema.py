from core.schema import CustomBaseModel
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe


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
