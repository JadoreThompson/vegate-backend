from vegate.core.schema import CustomBaseModel
from vegate.oms.enums import BrokerType
from ..enums import MarketType, Timeframe


class SubscribeRequest(CustomBaseModel):
    symbol: str
    market_type: MarketType
    timeframe: list[Timeframe]
    broker_type: BrokerType
