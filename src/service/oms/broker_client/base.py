from abc import ABC, abstractmethod

from service.oms.broker_client.model import Order, OrderRequest


class BrokerClient(ABC):
    """Abstract base class for broker implementations."""

    def __init__(self):
        pass

    def connect(self) -> None:
        """
        Tests the connection to the broker by fetching the balance
        """
        self.get_balance()

    def disconnect(self) -> None:
        return

    @abstractmethod
    def get_balance(self): ...

    @abstractmethod
    def get_equity(self): ...

    @abstractmethod
    def get_position(self, symbol: str): ...

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
            order_id: Id of order to cancel

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
