from typing import Any
from uuid import UUID

from core.enums import DeploymentEventType
from core.models import OHLCV, CustomBaseModel
from engine.enums import BrokerType, MarketType


class DeploymentEvent(CustomBaseModel):
    type: DeploymentEventType
    deployment_id: UUID


class BrokerTradeEvent(CustomBaseModel):
    """
    Emitted via the listener, contains the details
    for the trade. To be consumed to build ohlc levels
    and update the in memory cache of instrument prices
    """

    broker: BrokerType
    market_type: MarketType
    symbol: str
    price: float
    quantity: float
    timestamp: int
    broker_metadata: dict[str, Any] | None = None


# class CandleCloseEvent(CustomBaseModel):
#     """
#     Emitted when an OHLC candle closes.
#     Contains the complete candle data for a specific timeframe.
#     """

#     broker: BrokerType
#     symbol: str
#     timeframe: Timeframe
#     timestamp: str  # ISO format datetime
#     open: str  # Decimal as string
#     high: str  # Decimal as string
#     low: str  # Decimal as string
#     close: str  # Decimal as string
#     volume: int


class CandleCloseEvent(OHLCV):
    broker: BrokerType
