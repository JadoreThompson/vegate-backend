from abc import ABC, abstractmethod

from enums import BrokerType
from service.ohlc.loader.loader_config import OHLCLoaderConfig


class BaseOHLCLoader(ABC):
    """Abstract base class for historical candle loaders."""

    def __init__(self, broker_type: BrokerType):
        self.broker_type = broker_type

    @abstractmethod
    async def run(self, config: OHLCLoaderConfig) -> None:
        """Run the loader with the given configuration."""
        pass
