"""Base strategy class for trading strategies."""

import logging
from abc import ABC, abstractmethod

from lib.brokers import BaseBroker
from models import OHLC


class BaseStrategy(ABC):
    """Abstract base class for trading strategies."""

    def __init__(self, name: str, broker: BaseBroker):
        """Initialize the strategy.

        Args:
            name: Name of the strategy (used as logger name)
            broker: Broker instance for placing orders
        """
        self._name = name
        self.broker = broker
        self.logger = logging.getLogger(name)

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def on_candle(self, candle: OHLC) -> None:
        """Called when a new candle is received.

        Args:
            candle: OHLC candle
        """
        pass

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
