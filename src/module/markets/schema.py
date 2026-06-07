from datetime import datetime
from uuid import UUID

from core.schema import CustomBaseModel
from vegate.markets.enums import MarketType, Timeframe
from vegate.oms.enums import BrokerType


class InstrumentInfo(CustomBaseModel):
    id: UUID
    symbol: str
    broker_type: BrokerType
    market_type: MarketType
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime
