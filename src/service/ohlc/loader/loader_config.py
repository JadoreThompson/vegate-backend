from datetime import date
from typing import TYPE_CHECKING, NamedTuple, Type

from enums import MarketType, Timeframe
if TYPE_CHECKING:
    from service.ohlc.loader.base import BaseOHLCLoader


class LoaderConfig(NamedTuple):
    cls: Type["BaseOHLCLoader"]
    symbol: str
    market_type: MarketType
    timeframe: Timeframe
    start_date: date
    end_date: date
    poll_interval: int
