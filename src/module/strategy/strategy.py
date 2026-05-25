import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from module.event_bus import SyncEventPublisher
from module.markets.feed import OHLCFeedClient
from module.markets.historical import HistoricalDataClient
from module.markets.schema import OHLC

if TYPE_CHECKING:
    from module.deployment.oms import OMSClient


class BaseStrategy(ABC):

    def __init__(
        self,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: "OMSClient",
        event_publisher: SyncEventPublisher,
        historical_data_client: HistoricalDataClient | None = None,
    ):
        self.ohlc_feed_client = ohlc_feed_client
        self.oms_client = oms_client
        self.event_publisher = event_publisher
        self.historical_data_client = historical_data_client
        self.name = "Strategy"
        self.logger = logging.getLogger(self.name)

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    @abstractmethod
    def on_candle(self, candle: OHLC): ...

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
