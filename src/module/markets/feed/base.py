from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from vegate.oms.enums import BrokerType
from vegate.markets.enums import MarketType, Timeframe
from ..model import OHLC


class OHLCFeed(ABC):

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def symbols(self) -> list[str]: ...

    @property
    @abstractmethod
    def broker(self) -> BrokerType: ...

    @property
    @abstractmethod
    def timeframes(self) -> list[Timeframe]: ...

    @property
    @abstractmethod
    def market_type(self) -> MarketType: ...

    @abstractmethod
    async def run(self): ...

    @abstractmethod
    async def stop(self): ...

    @abstractmethod
    def set_on_candle(self, func: Callable[[OHLC], Any | Awaitable[Any]]) -> None: ...
