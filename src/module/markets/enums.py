from enum import Enum


class MarketType(str, Enum):
    """Types of markets."""

    STOCKS = "stocks"
    FOREX = "forex"
    CRYPTO = "crypto"


class Timeframe(str, Enum):
    """Supported timeframes for OHLC data."""

    m1 = "1m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"

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
        else:
            raise ValueError(f"Unknown timeframe unit: {unit}")

    def to_seconds(self) -> int:
        """Alias for get_seconds() for compatibility."""
        return self.get_seconds()
