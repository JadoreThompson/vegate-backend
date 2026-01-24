from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Generator

from models import Order, OrderRequest, OHLC
from enums import Timeframe


class BaseBroker(ABC):
    """Abstract base class for broker implementations."""

    supports_async: bool = False

    def __init__(self):
        self.broker = None

    @abstractmethod
    def get_balance(self): ...

    @abstractmethod
    def get_equity(self): ...

    @abstractmethod
    def place_order(self, order_request: OrderRequest) -> Order:
        """Place an order.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        pass

    @abstractmethod
    def modify_order(
        self,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        """Modify an existing order.

        Args:
            order_id: ID of order to modify
            limit_price: New limit price (optional)
            stop_price: New stop price (optional)

        Returns:
            Modified Order object
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        pass

    @abstractmethod
    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully, False otherwise
        """
        pass

    @abstractmethod
    def get_order(self, order_id: str) -> Order | None:
        """Get a specific order.

        Args:
            order_id: ID of order to retrieve

        Returns:
            Order object or None if not found
        """
        pass

    @abstractmethod
    def get_orders(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of Order objects
        """
        pass

    @abstractmethod
    def stream_candles(
        self, symbol: str, timeframe: Timeframe
    ) -> Generator[OHLC, None, None]:
        """Stream candles synchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        pass

    @abstractmethod
    async def stream_candles_async(
        self, symbol: str, timeframe: Timeframe
    ) -> AsyncGenerator[OHLC, None]:
        """Stream candles asynchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        pass
