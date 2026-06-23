from enum import Enum


class OrderType(str, Enum):
    """Supported order types across all brokers."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class ContractType(str, Enum):
    PERPETUAL = "perpetual"
    FUTURE = "future"


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


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
    TRADIER = "tradier"
    CTRADER = "ctrader"
