from dataclasses import dataclass

from alpaca.data import timeframe

from enums import MarketType, Timeframe


# @dataclass(slots=True, frozen=True)
# class StrategyConfig:
#     symbol: str
#     market_type: MarketType
#     timeframe: Timeframe


class StrategyConfig:

    def __init__(self, symbol: str, market_type: MarketType, timeframe: Timeframe):
        self._symbol = symbol
        self._market_type = market_type
        self._timeframe = timeframe

    @property
    def symbol(self):
        return self._symbol

    @property
    def market_type(self):
        return self._market_type

    @property
    def timeframe(self):
        return self._timeframe
