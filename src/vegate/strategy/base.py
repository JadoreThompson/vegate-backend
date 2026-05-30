import logging
from abc import ABC, abstractmethod

from vegate.markets.feed.client import OHLCFeedClient
from vegate.markets.historical.client import HistoricalDataClient
from vegate.markets.schema import OHLC

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from vegate.oms.client import OMSClient


class BaseStrategy(ABC):

    def __init__(
        self,
        *,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: "OMSClient",
        historical_data_client: HistoricalDataClient,
    ):
        self.ohlc_feed_client = ohlc_feed_client
        self.oms_client = oms_client
        self.historical_data_client = historical_data_client

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    @abstractmethod
    def on_candle(self, candle: OHLC): ...

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
