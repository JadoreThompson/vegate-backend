from dataclasses import dataclass

from enums import MarketType, Timeframe


@dataclass(slots=True, frozen=True)
class StrategyConfig:
    symbol: str
    market_type: MarketType
    timeframe: Timeframe
