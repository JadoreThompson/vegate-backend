from abc import ABC, abstractmethod
from typing import Optional, List
import logging

from ..models import OrderRequest, OrderResponse, Position, Account
from .rate_limiter import TokenBucketRateLimiter
from .exc import BrokerError

logger = logging.getLogger(__name__)


class BaseBroker(ABC):
    """
    Abstract base class for all broker implementations.

    This class defines the interface that all broker implementations must provide.
    It includes both abstract methods that must be implemented by subclasses and
    concrete methods that provide common functionality like context management
    and rate limiting.

    Lifecycle:
        1. __init__() - Initialize with credentials
        2. connect() - Establish connection and authenticate
        3. ... use broker methods ...
        4. disconnect() - Clean up resources

    Alternative lifecycle using context manager:
        with broker:
            # Use broker methods
            broker.submit_order(order)

    Attributes:
        rate_limiter: Optional rate limiter for API calls
        _connected: Connection state flag

    Example:
        class MyBroker(BaseBroker):
            def connect(self):
                # Implementation here
                pass

            def submit_order(self, order):
                # Implementation here
                pass
    """

    def __init__(self, rate_limiter: Optional[TokenBucketRateLimiter] = None):
        """
        Initialize broker with optional rate limiter.

        Args:
            rate_limiter: Optional rate limiter for API calls.
                         If None, no rate limiting is applied.
        """
        self.rate_limiter = rate_limiter
        self._connected = False

    # Lifecycle Management

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to broker and authenticate.

        This method should:
        - Validate credentials
        - Establish connection to broker API
        - Perform any necessary authentication
        - Set up any required resources

        Raises:
            AuthenticationError: If authentication fails
            ConnectionError: If connection cannot be established
            BrokerError: For other connection-related errors
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Gracefully disconnect from broker.

        This method should:
        - Close any open connections
        - Clean up resources
        - Cancel any pending subscriptions

        This method should not raise exceptions and should be safe
        to call multiple times.
        """
        pass

    def __enter__(self):
        """
        Context manager entry.

        Automatically connects to the broker when entering the context.

        Returns:
            Self for use in the context

        Example:
            with broker:
                broker.submit_order(order)
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit.

        Automatically disconnects from the broker when exiting the context,
        even if an exception occurred.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False to propagate any exception that occurred
        """
        self.disconnect()
        return False

    # Order Management

    @abstractmethod
    def submit_order(self, order: OrderRequest) -> OrderResponse:
        """
        Submit an order to the broker.

        Args:
            order: Order request with all necessary parameters

        Returns:
            Order response with order details and status

        Raises:
            OrderRejectedError: If the order is rejected
            InsufficientFundsError: If insufficient funds
            RateLimitError: If rate limit is exceeded
            BrokerError: For other submission errors
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.

        Args:
            order_id: Broker-assigned order identifier

        Returns:
            True if order was successfully cancelled, False otherwise

        Raises:
            BrokerError: If cancellation fails for reasons other than
                        order already filled or not found
        """
        pass

    @abstractmethod
    def get_order(self, order_id: str) -> OrderResponse:
        """
        Get current status of an order.

        Args:
            order_id: Broker-assigned order identifier

        Returns:
            Current order details and status

        Raises:
            BrokerError: If order cannot be retrieved
        """
        pass

    @abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderResponse]:
        """
        Get all open orders, optionally filtered by symbol.

        Args:
            symbol: Optional symbol to filter orders. If None, returns
                   all open orders across all symbols.

        Returns:
            List of open orders

        Raises:
            BrokerError: If orders cannot be retrieved
        """
        pass

    # Position Management

    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get current position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position information if a position exists, None otherwise

        Raises:
            BrokerError: If position data cannot be retrieved
        """
        pass

    @abstractmethod
    def get_all_positions(self) -> List[Position]:
        """
        Get all current positions.

        Returns:
            List of all current positions across all symbols

        Raises:
            BrokerError: If position data cannot be retrieved
        """
        pass

    @abstractmethod
    def close_position(self, symbol: str) -> OrderResponse:
        """
        Close entire position for a symbol.

        This is a convenience method that submits a market order to close
        the position. The order quantity is determined by the current position.

        Args:
            symbol: Trading symbol

        Returns:
            Order response for the closing order

        Raises:
            BrokerError: If position cannot be closed or doesn't exist
            OrderRejectedError: If closing order is rejected
        """
        pass

    # Account Information

    @abstractmethod
    def get_account(self) -> Account:
        """
        Get current account information.

        Returns:
            Account information including balances and buying power

        Raises:
            BrokerError: If account data cannot be retrieved
        """
        pass

    # Helper Methods

    def _apply_rate_limit(self) -> None:
        """
        Apply rate limiting if configured.

        This helper method should be called before making API requests
        to ensure rate limits are respected.
        """
        if self.rate_limiter:
            self.rate_limiter.acquire()

    def _log_error(self, operation: str, error: Exception) -> None:
        """
        Log broker errors with context.

        Args:
            operation: Name of the operation that failed
            error: The exception that occurred
        """
        logger.error(
            f"Broker error during {operation}: {error}",
            extra={
                "operation": operation,
                "error_type": type(error).__name__,
                "broker_type": type(self).__name__,
            },
            exc_info=True,
        )
