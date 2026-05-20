from datetime import datetime
from uuid import UUID

from enums import BrokerType, Timeframe, MarketType
from models import CustomBaseModel


class InstrumentInfo(CustomBaseModel):
    id: UUID
    symbol: str
    broker_type: BrokerType
    market_type: MarketType
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime
