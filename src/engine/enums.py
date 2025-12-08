from enum import Enum


class Timeframe(str, Enum):
    """Supported timeframes for OHLC data."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    D1 = "1d"

    def get_seconds(self) -> int:
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


class OrderType(str, Enum):
    """Supported order types across all brokers."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(str, Enum):
    """Order side: buy or sell."""

    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Order execution status."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(str, Enum):
    """Time-in-force options for orders."""

    DAY = "day"  # Valid for the trading day
    GTC = "gtc"  # Good till cancelled
    IOC = "ioc"  # Immediate or cancel
    FOK = "fok"  # Fill or kill


class BacktestStatus(str, Enum):
    """Status of backtest execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LiveDeploymentStatus(str, Enum):
    """Status of live deployment."""

    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"


class BrokerType(str, Enum):
    """Supported broker platforms."""

    ALPACA = "alpaca"


class MarketType(str, Enum):
    STOCKS = "stocks"
    CRYPTO = "crypto"
