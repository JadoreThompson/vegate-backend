import logging
from typing import List, Iterator
from datetime import datetime
from enum import Enum

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..models import OrderSide

logger = logging.getLogger(__name__)


class Timeframe(str, Enum):
    """Supported timeframes for OHLC data."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    D1 = "1d"


class OHLCBar:
    """
    Single OHLC bar representing price data for a specific timeframe.

    Attributes:
        symbol: Trading symbol (e.g., "AAPL")
        timestamp: Bar timestamp
        open: Opening price
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price
        volume: Trading volume
        timeframe: Bar timeframe
    """

    def __init__(
        self,
        symbol: str,
        timestamp: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        timeframe: Timeframe,
    ):
        self.symbol = symbol
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.timeframe = timeframe

    def __repr__(self) -> str:
        return (
            f"OHLCBar(symbol={self.symbol}, timestamp={self.timestamp}, "
            f"open={self.open}, high={self.high}, low={self.low}, "
            f"close={self.close}, volume={self.volume})"
        )


class TradeRecord:
    """
    Record of a single trade execution.

    Attributes:
        trade_id: Unique trade identifier
        symbol: Trading symbol
        side: Buy or sell
        entry_time: When position was entered
        entry_price: Entry price
        exit_time: When position was exited (None if still open)
        exit_price: Exit price (None if still open)
        quantity: Number of shares
        pnl: Profit/loss in dollars
        commission: Total commission paid
        slippage: Total slippage cost
    """

    def __init__(
        self,
        trade_id: str,
        symbol: str,
        side: OrderSide,
        entry_time: datetime,
        entry_price: float,
        quantity: float,
        exit_time: datetime = None,
        exit_price: float = None,
        pnl: float = 0.0,
        commission: float = 0.0,
        slippage: float = 0.0,
    ):
        self.trade_id = trade_id
        self.symbol = symbol
        self.side = side
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.quantity = quantity
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.pnl = pnl
        self.commission = commission
        self.slippage = slippage


class OHLCDataLoader:
    """
    Loads historical OHLC data from PostgreSQL database.

    This class provides efficient batch loading of OHLC data for backtesting,
    handling multiple symbols and timeframes with proper error handling.

    Example:
        with get_db_sess() as session:
            loader = OHLCDataLoader(session)
            for batch in loader.load_data(
                symbols=["AAPL", "TSLA"],
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 12, 31),
                timeframe=Timeframe.MINUTE_1
            ):
                for bar in batch:
                    print(f"{bar.symbol}: {bar.close}")
    """

    def __init__(self, db_session: Session):
        """
        Initialize data loader with database session.

        Args:
            db_session: Database session from SQLAlchemy
        """
        self.db_session = db_session
        logger.info("OHLCDataLoader initialized")

    def load_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: Timeframe,
        batch_size: int = 10000,
    ) -> Iterator[List[OHLCBar]]:
        """
        Load OHLC data in batches for memory efficiency.

        Yields batches of OHLC bars in chronological order across all symbols.
        This ensures proper event-driven simulation where all symbols are
        processed at each timestamp.

        Args:
            symbols: List of trading symbols to load
            start_date: Start date for data (inclusive)
            end_date: End date for data (inclusive)
            timeframe: Bar timeframe
            batch_size: Number of bars per batch (default: 10000)

        Yields:
            Batches of OHLCBar objects in chronological order

        Raises:
            ValueError: If no symbols provided or invalid date range
            Exception: If database query fails
        """
        if not symbols:
            raise ValueError("At least one symbol must be provided")

        if start_date >= end_date:
            raise ValueError("start_date must be before end_date")

        logger.info(
            f"Loading OHLC data: symbols={symbols}, "
            f"start={start_date}, end={end_date}, "
            f"timeframe={timeframe.value}, batch_size={batch_size}"
        )

        # Query to fetch OHLC data in chronological order
        # Note: Assuming table name 'ohlc_data' with columns:
        # symbol, timestamp, timeframe, open, high, low, close, volume
        query = text(
            """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM ohlc_data
            WHERE symbol = ANY(:symbols)
              AND timestamp >= :start_date
              AND timestamp <= :end_date
              AND timeframe = :timeframe
            ORDER BY timestamp ASC, symbol ASC
            LIMIT :batch_size OFFSET :offset
        """
        )

        offset = 0
        total_bars = 0

        try:
            while True:
                # Execute query with parameters
                result = self.db_session.execute(
                    query,
                    {
                        "symbols": symbols,
                        "start_date": start_date,
                        "end_date": end_date,
                        "timeframe": timeframe.value,
                        "batch_size": batch_size,
                        "offset": offset,
                    },
                )

                rows = result.fetchall()

                if not rows:
                    logger.info(
                        f"Data loading complete. Total bars loaded: {total_bars}"
                    )
                    break

                # Convert rows to OHLCBar objects
                bars = []
                for row in rows:
                    bar = OHLCBar(
                        symbol=row[0],
                        timestamp=row[1],
                        open=float(row[2]),
                        high=float(row[3]),
                        low=float(row[4]),
                        close=float(row[5]),
                        volume=int(row[6]),
                        timeframe=timeframe,
                    )
                    bars.append(bar)

                batch_count = len(bars)
                total_bars += batch_count
                logger.debug(f"Loaded batch: {batch_count} bars (offset={offset})")

                yield bars

                # Move to next batch
                offset += batch_size

                # If we got fewer bars than batch_size, we're done
                if batch_count < batch_size:
                    logger.info(
                        f"Data loading complete. Total bars loaded: {total_bars}"
                    )
                    break

        except Exception as e:
            logger.error(f"Error loading OHLC data: {e}", exc_info=True)
            raise

    def get_bar_count(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: Timeframe,
    ) -> int:
        """
        Get total count of bars for given parameters.

        Useful for progress tracking during backtests.

        Args:
            symbols: List of trading symbols
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            timeframe: Bar timeframe

        Returns:
            Total number of bars available
        """
        query = text(
            """
            SELECT COUNT(*)
            FROM ohlc_data
            WHERE symbol = ANY(:symbols)
              AND timestamp >= :start_date
              AND timestamp <= :end_date
              AND timeframe = :timeframe
        """
        )

        try:
            result = self.db_session.execute(
                query,
                {
                    "symbols": symbols,
                    "start_date": start_date,
                    "end_date": end_date,
                    "timeframe": timeframe.value,
                },
            )

            count = result.scalar()
            logger.debug(f"Bar count: {count} for {len(symbols)} symbols")
            return count or 0

        except Exception as e:
            logger.error(f"Error getting bar count: {e}", exc_info=True)
            return 0
