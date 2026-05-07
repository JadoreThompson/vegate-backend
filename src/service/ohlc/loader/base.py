from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator

from enums import BrokerType, MarketType, Timeframe
from models import OHLC


class BaseOHLCLoader(ABC):
    """Abstract base class for historical candle loaders."""

    def __init__(self, broker_type: BrokerType):
        self.broker_type = broker_type

    @abstractmethod
    async def load_candles(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        """Asynchronously load historical OHLC candles from Alpaca API and persist to database.

        Note:
            Alpaca's historical data client is synchronous, so this
            implementation wraps the synchronous call.

        Args:
            symbol: Trading symbol (e.g., "AAPL").
            market_type: Market category for the asset (e.g., stocks, crypto).
            timeframe: Candle timeframe (e.g., Timeframe.ONE_MINUTE).
            start_date: Start datetime for historical data range (inclusive).
            end_date: End datetime for historical data range (inclusive).
        """
        pass
