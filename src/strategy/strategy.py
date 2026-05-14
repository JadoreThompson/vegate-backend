import logging
from abc import ABC

from service.event.publisher import EventPublisherService
from service.ohlc.feed.client import OHLCFeedClient
from service.oms.client import OMSClient


class Strategy(ABC):

    def __init__(self, feed_client: OHLCFeedClient, oms_client: OMSClient, event_publisher: EventPublisherService):
        self._feed_client = feed_client
        self._oms_client = oms_client
        self._event_publisher = event_publisher
        self.name = "Strategy"
        self.logger = logging.getLogger(self.name)

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
