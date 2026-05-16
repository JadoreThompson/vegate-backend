from datetime import datetime

from enums import BrokerType, Timeframe, MarketType
from models import CustomBaseModel


class OHLCInfo(CustomBaseModel):
    symbol: str
    broker: BrokerType
    market_type: MarketType
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime
