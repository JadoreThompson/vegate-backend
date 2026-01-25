# from enum import Enum


# class OrderType(str, Enum):
#     """Order type enumeration."""

#     MARKET = "market"
#     LIMIT = "limit"
#     STOP = "stop"
#     STOP_LIMIT = "stop_limit"


# class OrderStatus(str, Enum):
#     """Order status enumeration."""

#     PENDING = "pending"
#     PLACED = "placed"
#     PARTIALLY_FILLED = "partially_filled"
#     FILLED = "filled"
#     CANCELLED = "cancelled"


# class OrderSide(str, Enum):
#     BUY = "buy"
#     SELL = "sell"


# class BrokerType(str, Enum):
#     """Broker type enumeration."""

#     ALPACA = "alpaca"


# class BacktestStatus(str, Enum):
#     """Backtest status enumeration."""

#     PENDING = "pending"
#     RUNNING = "running"
#     COMPLETED = "completed"
#     FAILED = "failed"


# class DeploymentStatus(str, Enum):
#     """Deployment status for a strategy"""

#     RUNNING = "running"
#     STOPPED = "stopped"
#     ERROR = "error"


# class Timeframe(str, Enum):
#     """Supported timeframes for OHLC data."""

#     m1 = "1m"
#     m5 = "5m"
#     m15 = "15m"
#     m30 = "30m"
#     H1 = "1h"
#     H4 = "4h"
#     D1 = "1d"

#     def get_seconds(self) -> int:
#         """Convert timeframe to seconds.

#         Returns:
#             Number of seconds in this timeframe

#         Raises:
#             ValueError: If timeframe unit is unknown
#         """
#         unit = self.value[-1]
#         amount = int(self.value[:-1])

#         if unit == "m":
#             return amount * 60
#         elif unit == "h":
#             return amount * 3600
#         elif unit == "d":
#             return amount * 86400
#         else:
#             raise ValueError(f"Unknown timeframe unit: {unit}")


from enum import Enum


class OrderType(str, Enum):
    """Supported order types across all brokers."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderStatus(str, Enum):
    """Order execution status."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    PLACED = "placed"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OrderSide(str, Enum):
    """Order side: buy or sell."""

    BUY = "buy"
    SELL = "sell"



class BrokerType(str, Enum):
    """Supported broker platforms."""

    ALPACA = "alpaca"


class BacktestStatus(str, Enum):
    """Status of backtest execution."""

    PENDING = "pending"
    RUNNING = "running"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DeploymentStatus(str, Enum):
    """Deployment status for a strategy."""

    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    STOP_REQUESTED = "stop_requested"


class DeploymentEventType(str, Enum):
    """Deployment event types."""

    START = "deployment_start"
    STOP = "deployment_stop"
    ERROR = "deployment_error"


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


class PricingTierType(str, Enum):
    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class SnapshotType(str, Enum):
    """Types of account snapshots."""

    EQUITY = "equity"
    BALANCE = "balance"