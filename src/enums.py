from enum import Enum


class OrderType(str, Enum):
    """Order type enumeration."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatus(str, Enum):
    """Order status enumeration."""

    PENDING = "pending"
    PLACED = "placed"
    PARTIALLY_FILLED = 'partially_filled'
    FILLED = "filled"
    CANCELLED = "cancelled"


class OrderSide(str, Enum):
    BUY = 'buy'
    SELL = 'sell'


class BrokerType(str, Enum):
    """Broker type enumeration."""

    ALPACA = "alpaca"


class BacktestStatus(str, Enum):
    """Backtest status enumeration."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Timeframe(str, Enum):
    """Supported timeframes for OHLC data."""

    m1 = "1m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"
    M1 = "1M"
    ONE_YEAR = "1y"

    def get_seconds(self) -> int:
        """Convert timeframe to seconds.

        Returns:
            Number of seconds in this timeframe

        Raises:
            ValueError: If timeframe unit is unknown
        """
        unit = self.value[-1]
        amount = int(self.value[:-1])

        if unit == "m":
            return amount * 60
        elif unit == "h":
            return amount * 3600
        elif unit == "d":
            return amount * 86400
        elif unit == "w":
            return amount * 604800
        elif unit == "M":
            return amount * 2592000
        elif unit == "y":
            return amount * 31_536_000
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")
