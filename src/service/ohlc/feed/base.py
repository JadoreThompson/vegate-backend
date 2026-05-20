from abc import ABC, abstractmethod
from typing import Any, Callable

from enums import BrokerType, MarketType, Timeframe
from models import OHLC


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
    async def run(self):
        ...

    @abstractmethod
    async def stop(self):
        ...
