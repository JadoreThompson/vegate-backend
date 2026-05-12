from datetime import datetime

from enums import BrokerType, Timeframe
from models import CustomBaseModel


class OHLCInfo(CustomBaseModel):
    symbol: str
    broker: BrokerType
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime
