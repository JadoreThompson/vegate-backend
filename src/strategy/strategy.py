import logging
from abc import ABC, abstractmethod

from service.event.publisher import EventPublisherService
from service.ohlc.feed.client import OHLCFeedClient
from service.oms.client import OMSClient
from strategy.model import StrategyConfig


class Strategy(ABC):

    def __init__(
        self,
        config: StrategyConfig,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: OMSClient,
        event_publisher: EventPublisherService,
    ):
        self._config = config
        self._feed_client = ohlc_feed_client
        self._oms_client = oms_client
        self._event_publisher = event_publisher
        self.name = "Strategy"
        self.logger = logging.getLogger(self.name)

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    @abstractmethod
    def run(self) -> None:
        """Run the strategy. Long running method"""
        pass

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
