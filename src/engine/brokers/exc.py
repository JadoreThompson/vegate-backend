from typing import Optional


class BrokerError(Exception):
    """
    Base exception for all broker-related errors.

    All broker implementations should raise exceptions derived from this class
    to enable consistent error handling across different brokers.

    Attributes:
        message: Human-readable error message
        broker_code: Broker-specific error code (optional)
        retry_after: Suggested wait time in seconds before retry (optional)
    """

    def __init__(
        self,
        message: str,
        broker_code: Optional[str] = None,
        retry_after: Optional[int] = None,
    ):
        """
        Initialize broker error.

        Args:
            message: Error message describing what went wrong
            broker_code: Broker-specific error code for debugging
            retry_after: Seconds to wait before retrying (for rate limits)
        """
        super().__init__(message)
        self.broker_code = broker_code
        self.retry_after = retry_after


class AuthenticationError(BrokerError):
    """
    Raised when broker authentication fails.

    This typically indicates invalid credentials, expired tokens,
    or insufficient permissions. This is usually a non-recoverable
    error requiring user intervention.
    """

    pass


class OrderRejectedError(BrokerError):
    """
    Raised when an order is rejected by the broker.

    Orders can be rejected for various reasons including:
    - Invalid order parameters
    - Symbol not tradeable
    - Quantity constraints violated
    - Price constraints violated
    - Market closed
    """

    pass


class RateLimitError(BrokerError):
    """
    Raised when broker API rate limit is exceeded.

    This error includes a retry_after field indicating how long
    to wait before making additional requests. Rate limiters should
    handle this automatically, but it may still occur under high load.
    """

    pass


class InsufficientFundsError(BrokerError):
    """
    Raised when account has insufficient funds for an operation.

    This occurs when attempting to place an order that would exceed
    available buying power or attempting to sell more shares than held.
    """

    pass


class ConnectionError(BrokerError):
    """
    Raised when connection to broker cannot be established or is lost.

    This may be due to network issues, broker service outages,
    or other connectivity problems.
    """

    pass


class SymbolNotFoundError(BrokerError):
    """
    Raised when a trading symbol is not found or not supported.

    This occurs when attempting to trade or get data for a symbol
    that doesn't exist or isn't available through the broker.
    """

    pass


class DataNotAvailableError(BrokerError):
    """
    Raised when requested market data is not available.

    This can occur when requesting historical data outside the
    available date range or for symbols without data.
    """

    pass
