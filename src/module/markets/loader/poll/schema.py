from dataclasses import dataclass
from datetime import datetime

from vegate.markets.enums import MarketType, Timeframe
from vegate.oms.enums import BrokerType


@dataclass
class PollSubscription:
    broker: BrokerType
    symbol: str
    market_type: MarketType
    timeframes: list[Timeframe]
    start_date: datetime
    end_date: datetime | None = None