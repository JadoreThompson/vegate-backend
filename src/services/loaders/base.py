from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator

from enums import BrokerType, Timeframe
from models import OHLC


class BaseLoader(ABC):
    """Abstract base class for historical candle loaders."""

    def __init__(self, broker_type: BrokerType):
        self.broker_type = broker_type

    @abstractmethod
    async def load_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> AsyncIterator[OHLC]:
        """
        Asynchronously load historical candles from broker API.

        Args:
            symbol: Trading symbol (e.g., 'AAPL')
            timeframe: Candle timeframe (e.g., Timeframe.ONE_MINUTE)
            start_date: Start date for historical data
            end_date: End date for historical data

        Yields:
            OHLC candles in chronological order
        """
        pass
