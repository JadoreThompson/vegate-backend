from abc import ABC, abstractmethod


class Strategy(ABC):

    def __init__(
        self,
        ohlc_feed_client: OHLCFeedClient,
        broker_client: BrokerClient,
        event_publisher: SyncEventPublisher,
    ):
        self.config = config
        self.ohlc_feed_client = ohlc_feed_client
        self.oms_client = broker_client
        self.event_publisher = event_publisher
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
