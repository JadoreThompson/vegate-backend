from datetime import datetime
import uuid
from enums import OrderStatus, OrderType, BrokerType, Timeframe
from models import Order, OrderRequest, OHLC
from .base import BaseBroker
from infra.db import get_db_sess_sync
from infra.db.models import OHLCs


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
        # if order_request.order_type == OrderType.LIMIT:
        #     if order_request.limit_price is None:
        #         raise ValueError("Limit pice must be set for order with type limit")
        #     if order_request.limit_price <= 0.0:
        #         raise ValueError("Limit price must be greater than 0.0")
        # if order_request.order_type == OrderType.STOP:
        #     if order_request.stop_price is None:
        #         raise ValueError("Stop pice must be set for order with type stop")
        #     if order_request.stop_price <= 0.0:
        #         raise ValueError("Stop price must be greater than 0.0")

        order_id = str(uuid.uuid4())
        now = datetime.now()

        order = Order(
            symbol=order_request.symbol,
            quantity=order_request.quantity,
            executed_quantity=order_request.quantity,
            notional=order_request.notional,
            order_type=order_request.order_type,
            side=order_request.side,
            price=order_request.price,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            executed_at=now,
            submitted_at=now,
            order_id=order_id,
            status=OrderStatus.FILLED,
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

    def stream_candles(self, symbol, timeframe):
        raise NotImplementedError()

    async def stream_candles_async(self, symbol, timeframe):
        raise NotImplementedError()
