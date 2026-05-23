from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC


class OHLCFeed(ABC):

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def symbol(self) -> str: ...

    @property
    @abstractmethod
    def broker(self) -> BrokerType: ...

    @property
    @abstractmethod
    def timeframe(self) -> Timeframe: ...

    @property
    @abstractmethod
    def market_type(self) -> MarketType: ...

    @abstractmethod
    async def run(self): ...

    @abstractmethod
    async def stop(self): ...

    @abstractmethod
    def set_on_candle(self, func: Callable[[OHLC], Any | Awaitable[Any]]) -> None: ...
