from datetime import datetime
import uuid
from enums import OrderStatus, OrderType
from models import Order, OrderRequest
from .base import BaseBroker


class BacktestBroker(BaseBroker):
    """Broker implementation for backtesting."""

    supports_async = False

    def __init__(self, starting_balance: float):
        """Initialize backtesting broker.

        Args:
            starting_balance: Starting account balance
        """
        self.starting_balance = starting_balance
        self.balance = starting_balance
        self.orders: list[Order] = []
        self._order_map: dict[str, Order] = {}
        self.buy_orders: list[Order] = []
        self.sell_orders: list[Order] = []

    def place_order(self, order_request: OrderRequest) -> Order:
        """Place an order.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        order_id = str(uuid.uuid4())
        order = Order(
            symbol=order_request.symbol,
            quantity=order_request.quantity,
            notional=order_request.notional,
            order_type=order_request.order_type,
            price=order_request.price,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            executed_at=order_request.executed_at,
            submitted_at=order_request.submitted_at or datetime.now(),
            order_id=order_id,
            status=OrderStatus.PLACED,
        )

        self.orders.append(order)
        self._order_map[order_id] = order

        # Track buy/sell orders for PnL calculation
        if order_request.notional > 0:
            self.buy_orders.append(order)
        else:
            self.sell_orders.append(order)

        return order

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
        order = self._order_map.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")

        if limit_price is not None and order.order_type in (
            OrderType.LIMIT,
            OrderType.STOP_LIMIT,
        ):
            order.limit_price = limit_price

        if stop_price is not None and order.order_type in (
            OrderType.STOP,
            OrderType.STOP_LIMIT,
        ):
            order.stop_price = stop_price

        return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled successfully
        """
        order = self._order_map.get(order_id)
        if order and order.status == OrderStatus.PLACED:
            order.status = OrderStatus.CANCELLED
            return True
        return False

    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully
        """
        for order in self.orders:
            if order.status == OrderStatus.PLACED:
                order.status = OrderStatus.CANCELLED
        return True

    def get_order(self, order_id: str) -> Order | None:
        """Get a specific order.

        Args:
            order_id: ID of order to retrieve

        Returns:
            Order object or None if not found
        """
        return self._order_map.get(order_id)

    def get_orders(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of Order objects
        """
        return self.orders.copy()

    def stream_candles(self, symbol: str, timeframe: str):
        """Stream candles synchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        # This will be implemented by subclasses or the engine
        raise NotImplementedError("Use BacktestEngine to stream candles")

    async def stream_candles_async(self, symbol: str, timeframe: str):
        """Stream candles asynchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        # This will be implemented by subclasses or the engine
        raise NotImplementedError("Use BacktestEngine to stream candles")
