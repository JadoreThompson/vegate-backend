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
    FILLED = "filled"
    CANCELLED = "cancelled"


class BrokerType(str, Enum):
    """Broker type enumeration."""

    ALPACA = "alpaca"
