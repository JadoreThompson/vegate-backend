"""
Broker system for the trading strategy framework.

This package provides a unified interface for interacting with different
trading brokers, supporting both live and paper trading.
"""

from .base import BaseBroker
from .exc import (
    BrokerError,
    AuthenticationError,
    OrderRejectedError,
    InsufficientFundsError,
    RateLimitError,
    ConnectionError,
    SymbolNotFoundError,
    DataNotAvailableError,
)
from .rate_limiter import TokenBucketRateLimiter
from .factory import BrokerFactory
from .alpaca import AlpacaBroker

# Register Alpaca broker with factory
BrokerFactory.register("alpaca", AlpacaBroker)

__all__ = [
    # Base classes
    "BaseBroker",
    # Exceptions
    "BrokerError",
    "AuthenticationError",
    "OrderRejectedError",
    "InsufficientFundsError",
    "RateLimitError",
    "ConnectionError",
    "SymbolNotFoundError",
    "DataNotAvailableError",
    # Utilities
    "TokenBucketRateLimiter",
    "BrokerFactory",
    # Broker implementations
    "AlpacaBroker",
]
