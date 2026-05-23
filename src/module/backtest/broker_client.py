import logging
import uuid
from collections import defaultdict

# from enums import OrderSide, OrderStatus, OrderType
from module.broker import Order, OrderRequest
from module.broker.enums import OrderSide, OrderStatus, OrderType
from module.broker.client import BrokerClientException
from module.deployment.oms import OMSClient
# from module.markets.schema import OHLC as OHLCSchema
from module.markets.schema import OHLC as OHLCSchema
from .ohlc_feed_client import BacktestOHLCFeedClient


class BacktestBrokerClient(OMSClient):
    """OMS client implementation for backtesting."""

    def __init__(self, starting_balance: float):
        """Initialize backtesting broker.

        Args:
            starting_balance: Starting account balance
        """
        self.starting_balance = starting_balance
        self.equity = starting_balance
        self.balance = starting_balance
        self._order_map: dict[str, Order] = {}
        self._pending_orders: list[Order] = []
        self._asset_holdings: dict[str, float] = defaultdict(float)
        self.ohlc_feed_client: BacktestOHLCFeedClient | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_balance(self):
        return self.balance

    def get_equity(self):
        self.equity = self._calculate_equity()
        return self.equity
    
    def get_position(self,symbol: str):
        return self._asset_holdings.get(symbol, 0.0)

    def place_order(self, request: OrderRequest, candle_ts: int) -> Order:
        """Place an order.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        self._ensure_feed()

        if request.order_type == OrderType.LIMIT:
            return self._handle_limit_order(request)
        if request.order_type == OrderType.STOP:
            return self._handle_stop_order(request)
        if request.order_type == OrderType.MARKET:
            return self._handle_market_order(request)
        raise ValueError(f"Unsupported order type: {request.order_type}")

    def _handle_limit_order(self, order_request: OrderRequest) -> Order:
        """Handle limit order with validation and add to pending orders.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object with PLACED status

        Raises:
            ValueError: If validation fails
        """
        if order_request.limit_price is None:
            raise ValueError("Limit price must be set for limit orders")
        if order_request.limit_price <= 0.0:
            raise ValueError("Limit price must be greater than 0.0")
        if self.ohlc_feed_client.cur_candle is None:
            raise ValueError("Cannot place limit order without current market price")

        current_price = self.ohlc_feed_client.cur_candle.close

        # Validate limit price based on order side
        if order_request.side == OrderSide.BUY:
            if order_request.limit_price >= current_price:
                raise ValueError(
                    f"Buy limit price ({order_request.limit_price}) must be lower than current price ({current_price})"
                )
        else:
            if order_request.limit_price <= current_price:
                raise ValueError(
                    f"Sell limit price ({order_request.limit_price}) must be higher than current price ({current_price})"
                )

        order_id = str(uuid.uuid4())

        order = Order(
            symbol=order_request.symbol,
            quantity=order_request.quantity,
            filled_quantity=0.0,
            notional=order_request.notional,
            order_type=order_request.order_type,
            side=order_request.side,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            avg_fill_price=None,
            executed_at=None,
            submitted_at=self.ohlc_feed_client.cur_candle.timestamp,
            id=order_id,
            status=OrderStatus.PLACED,
        )

        self._pending_orders.append(order)
        self._order_map[order_id] = order

        return order

    def _handle_stop_order(self, order_request: OrderRequest) -> Order:
        """Handle stop order with validation and add to pending orders.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object with PLACED status

        Raises:
            ValueError: If validation fails
        """
        if order_request.stop_price is None:
            raise ValueError("Stop price must be set for stop orders")
        if order_request.stop_price <= 0.0:
            raise ValueError("Stop price must be greater than 0.0")
        if self.ohlc_feed_client.cur_candle is None:
            raise ValueError("Cannot place stop order without current market price")

        current_price = self.ohlc_feed_client.cur_candle.close

        # Validate stop price based on order side (opposite of limit orders)
        if order_request.side == OrderSide.BUY:
            if order_request.stop_price <= current_price:
                raise ValueError(
                    f"Buy stop price ({order_request.stop_price}) must be higher than current price ({current_price})"
                )
        else:  # SELL
            if order_request.stop_price >= current_price:
                raise ValueError(
                    f"Sell stop price ({order_request.stop_price}) must be lower than current price ({current_price})"
                )

        order_id = str(uuid.uuid4())

        order = Order(
            symbol=order_request.symbol,
            quantity=order_request.quantity,
            filled_quantity=0.0,
            notional=order_request.notional,
            order_type=order_request.order_type,
            side=order_request.side,
            limit_price=order_request.limit_price,
            stop_price=order_request.stop_price,
            avg_fill_price=None,
            executed_at=None,
            submitted_at=self.ohlc_feed_client.cur_candle.timestamp,
            id=order_id,
            status=OrderStatus.PLACED,
        )

        self._pending_orders.append(order)
        self._order_map[order_id] = order

        return order

    def _handle_market_order(self, request: OrderRequest) -> Order:
        """Handle market order with balance validation.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object with FILLED or REJECTED status
        """
        self._ensure_feed()
        self._logger.info(f"Balance {self.balance}")
        order_id = str(uuid.uuid4())
        price = self.ohlc_feed_client.cur_candle.close
        current_ts = self.ohlc_feed_client.cur_candle.timestamp

        # Calculate order cost
        if request.notional is not None and request.notional > 0:
            order_cost = request.notional
        else:
            order_cost = request.quantity * price

        # Check balance for buy orders
        if request.side == OrderSide.BUY:
            if self.balance < order_cost:
                # Insufficient balance - reject order
                order = Order(
                    symbol=request.symbol,
                    quantity=request.quantity,
                    filled_quantity=0.0,
                    notional=request.notional,
                    order_type=request.order_type,
                    side=request.side,
                    # price=order_request.price,
                    limit_price=request.limit_price,
                    stop_price=request.stop_price,
                    avg_fill_price=None,
                    executed_at=None,
                    submitted_at=current_ts,
                    id=order_id,
                    status=OrderStatus.REJECTED,
                )
                self._order_map[order_id] = order
                raise BrokerClientException("Insufficient balance")

        elif self._asset_holdings[request.symbol] < request.quantity:
            raise BrokerClientException("Insufficient asset holdings")

        # Sufficient balance - fill order
        if request.notional is not None:
            quantity = request.notional / self.ohlc_feed_client.cur_candle.close
        else:
            quantity = request.quantity

        if request.quantity is not None:
            notional = request.quantity * self.ohlc_feed_client.cur_candle.close
        else:
            notional = request.notional

        order = Order(
            symbol=request.symbol,
            quantity=round(quantity, 2),
            filled_quantity=round(quantity, 2),
            notional=round(notional, 2),
            order_type=request.order_type,
            side=request.side,
            limit_price=request.limit_price,
            stop_price=request.stop_price,
            avg_fill_price=price,
            executed_at=current_ts,
            submitted_at=current_ts,
            id=order_id,
            status=OrderStatus.FILLED,
        )

        self._order_map[order_id] = order

        # Update balance using order_cost (which accounts for notional)
        if order.side == OrderSide.BUY:
            self.balance -= order_cost
            self._asset_holdings[order.symbol] += order.filled_quantity
        else:
            self.balance += order_cost
            self._asset_holdings[order.symbol] -= order.filled_quantity

        return order

    def execute_pending_orders(self, candle: OHLCSchema):
        """Execute pending limit and stop orders based on current candle.

        Checks each pending order to see if it should be triggered based on
        current price, validates balance, and either fills or rejects the order.
        """
        self._ensure_feed()

        current_ts = candle.timestamp
        current_high = candle.high
        current_low = candle.low

        orders_to_remove = []

        for order in self._pending_orders:
            should_execute = False
            execution_price = None

            # Check if limit order should be executed
            if order.order_type == OrderType.LIMIT:
                if order.side == OrderSide.BUY:
                    # Buy limit executes when price drops to or below limit price
                    if current_low <= order.limit_price:
                        should_execute = True
                        execution_price = order.limit_price
                else:  # SELL
                    # Sell limit executes when price rises to or above limit price
                    if current_high >= order.limit_price:
                        should_execute = True
                        execution_price = order.limit_price

            # Check if stop order should be executed
            elif order.order_type == OrderType.STOP:
                if order.side == OrderSide.BUY:
                    # Buy stop executes when price rises to or above stop price
                    if current_high >= order.stop_price:
                        should_execute = True
                        execution_price = order.stop_price
                else:  # SELL
                    # Sell stop executes when price drops to or below stop price
                    if current_low <= order.stop_price:
                        should_execute = True
                        execution_price = order.stop_price

            if should_execute:
                # Calculate order cost
                if order.notional is not None and order.notional > 0:
                    order_cost = order.notional
                else:
                    order_cost = order.quantity * execution_price

                # Check balance for buy orders
                if order.side == OrderSide.BUY:
                    if self.balance < order_cost:
                        # Insufficient balance - reject order
                        order.status = OrderStatus.REJECTED
                        order.executed_at = current_ts
                        orders_to_remove.append(order)
                        continue

                # Sufficient balance - fill order
                order.status = OrderStatus.FILLED
                order.filled_quantity = order.quantity
                order.avg_fill_price = execution_price
                order.executed_at = current_ts

                # Update balance
                if order.side == OrderSide.BUY:
                    self.balance -= order_cost
                    self._asset_holdings[order.symbol] += order.filled_quantity
                else:
                    self.balance += order_cost
                    self._asset_holdings[order.symbol] -= order.filled_quantity

                orders_to_remove.append(order)

        # Remove executed/rejected orders from pending list
        for order in orders_to_remove:
            self._pending_orders.remove(order)

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
        self._ensure_feed()
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
            # Remove from pending orders list if present
            if order in self._pending_orders:
                self._pending_orders.remove(order)
            return True
        return False

    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully
        """
        for order in list(self._order_map.values()):
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
        return list(self._order_map.values())

    def _calculate_equity(self):
        """Calculate current equity based on balance and holdings.

        Equity = current balance + (quantity owned * current price)

        If no current candle is available, equity defaults to current balance.
        """
        if self.ohlc_feed_client is None or self.ohlc_feed_client.cur_candle is None:
            return self.balance

        cur_price = self.ohlc_feed_client.cur_candle.close
        total_quantity = 0

        for order in list(self._order_map.values()):
            if order.status == OrderStatus.FILLED:
                if order.side == OrderSide.BUY:
                    total_quantity += order.filled_quantity
                else:
                    total_quantity -= order.filled_quantity

        holdings_value = total_quantity * cur_price
        return self.balance + holdings_value

    def _ensure_feed(self):
        if self.ohlc_feed_client is None:
            raise ValueError(
                "OHLC feed is not set. OHLC feed is required to track current close price."
            )
