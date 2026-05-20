from dataclasses import dataclass

from alpaca.data import timeframe

from enums import BrokerType, MarketType, Timeframe

# @dataclass(slots=True, frozen=True)
# class StrategyConfig:
#     symbol: str
#     market_type: MarketType
#     timeframe: Timeframe


class StrategyConfig:

    def __init__(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
    ):
        self._symbol = symbol
        self._market_type = market_type
        self._broker_type = broker_type
        self._timeframe = timeframe

    @property
    def symbol(self):
        return self._symbol

    @property
    def market_type(self):
        return self._market_type
    
    @property
    def broker_type(self):
        return self._broker_type

    @property
    def timeframe(self):
        return self._timeframe
