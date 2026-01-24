import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from enums import OrderSide, OrderStatus, OrderType
from infra.db import get_db_sess_sync
from infra.db.models import OHLCs
from models import Order, OrderRequest, OHLC
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
        self.equity = starting_balance
        self.balance = starting_balance
        self.orders: list[Order] = []
        self._order_map: dict[str, Order] = {}
        self.buy_orders: list[Order] = []
        self.sell_orders: list[Order] = []
        self.cur_candle: OHLC | None = None

    def get_balance(self):
        return self.balance

    def get_equity(self):
        self.equity = self._calculate_equity()
        return self.equity

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

        if order_request.order_type == OrderType.MARKET:
            price = order_request.price
        elif order_request.order_type == OrderType.LIMIT:
            price = order_request.limit_price
        else:
            price = order_request.stop_price

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
            filled_avg_price=price,
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

        if order.side == OrderSide.BUY:
            self.balance -= order.quantity * price
        else:
            self.balance += order.quantity * self._cur_candle.close

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

    def stream_candles(self, symbol, timeframe, source, start_date, end_date):
        with get_db_sess_sync() as db_sess:
            results = db_sess.scalars(
                select(OHLCs)
                .where(
                    OHLCs.source == source,
                    OHLCs.symbol == symbol,
                    OHLCs.timeframe == timeframe,
                    OHLCs.timestamp
                    >= int(
                        datetime(
                            year=start_date.year,
                            month=start_date.month,
                            day=start_date.day,
                            tzinfo=UTC,
                        ).timestamp()
                    ),
                    OHLCs.timestamp
                    <= int(
                        datetime(
                            year=end_date.year,
                            month=end_date.month,
                            day=end_date.day,
                            tzinfo=UTC,
                        ).timestamp()
                    ),
                )
                .order_by(OHLCs.timestamp.asc())
            )

            for res in results.yield_per(1000):
                candle = OHLC(
                    open=res.open,
                    high=res.high,
                    low=res.low,
                    close=res.close,
                    volume=0.0,
                    timestamp=res.timestamp,
                    timeframe=res.timeframe,
                    symbol=res.symbol,
                )
                self._cur_candle = candle
                yield candle

    async def stream_candles_async(self, symbol, timeframe):
        raise NotImplementedError()

    def _calculate_equity(self):
        """Calculate current equity based on balance and holdings.

        Equity = current balance + (quantity owned * current price)

        If no current candle is available, equity defaults to current balance.
        """
        if self._cur_candle is None:
            self.equity = self.balance
            return

        cur_price = self._cur_candle.close

        total_quantity = 0
        for order in self.orders:
            if order.status == OrderStatus.FILLED:
                if order.side == OrderSide.BUY:
                    total_quantity += order.executed_quantity
                else:
                    total_quantity -= order.executed_quantity

        holdings_value = total_quantity * cur_price
        return self.balance + holdings_value
