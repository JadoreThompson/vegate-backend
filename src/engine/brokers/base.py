import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator

from engine.enums import Timeframe
from engine.ohlcv import OHLCV
from engine.models import OrderRequest, OrderResponse, Account


class BaseBroker(ABC):
    def __init__(self):
        self._connected = False
        self._logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def connect(self) -> None:
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
    def get_open_orders(self, symbol: str | None = None) -> list[OrderResponse]:
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

    @abstractmethod
    def get_historic_olhcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        prev_bars: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[OHLCV]: ...

    @abstractmethod
    def yield_historic_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        prev_bars: int | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Generator[OHLCV, None, None]: ...

    @abstractmethod
    def yield_ohlcv(
        self, symbol: str, timeframe: Timeframe
    ) -> Generator[OHLCV, None, None]: ...

    def _apply_rate_limit(self) -> None:
        """
        Apply rate limiting if configured.

        This helper method should be called before making API requests
        to ensure rate limits are respected.
        """

    def _log_error(self, operation: str, error: Exception) -> None:
        """
        Log broker errors with context.

        Args:
            operation: Name of the operation that failed
            error: The exception that occurred
        """
        self._logger.error(
            f"Broker error during {operation}: {error}",
            extra={
                "operation": operation,
                "error_type": type(error).__name__,
                "broker_type": type(self).__name__,
            },
            exc_info=True,
        )
