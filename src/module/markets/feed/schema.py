from core.schema import CustomBaseModel
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe


class SubscribeRequest(CustomBaseModel):
    symbol: str
    market_type: MarketType
    timeframe: list[Timeframe]
    broker_type: BrokerType
