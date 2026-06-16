from abc import ABC, abstractmethod
from datetime import datetime

from vegate.markets.enums import MarketType, Timeframe
from .schema import OHLCLoadResult


class OHLCLoader(ABC):

    @abstractmethod
    async def load_candles(
        self,
        symbol: str,
        market_type: MarketType,
        timeframes: list[Timeframe],
        start_date: datetime,
        end_date: datetime,
    ) -> OHLCLoadResult: ...

    @abstractmethod
    async def close(self): ...
