from dataclasses import dataclass

from enums import BrokerType, MarketType, Timeframe


@dataclass(slots=True, frozen=True)
class StrategyConfig:
    symbol: str
    market_type: MarketType
    timeframe: Timeframe
    broker: BrokerType
