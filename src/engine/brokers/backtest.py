import logging
from uuid import uuid4

from engine.models import (
    OrderRequest,
    OrderResponse,
    Account,
    OrderSide,
    OrderStatus,
    OrderType,
)
from engine.ohlcv import OHLCV
from utils.db import get_db_sess_sync
from .base import BaseBroker
from .exc import BrokerError, InsufficientFundsError, OrderRejectedError

logger = logging.getLogger(__name__)


class BacktestBroker(BaseBroker):
    def __init__(self, starting_balance: float):
        self._cash = starting_balance
        self._orders: list[OrderResponse] = []
        self._orders: dict[str, OrderResponse] = {}
        self._pending_orders: dict[str, tuple[OrderRequest, OrderResponse]] = {}
        self._current_candle: OHLCV | None = None
        self._account_id = f"ac_{uuid4()}"
        logger.info(f"SimulatedBroker initialized: capital=${starting_balance:,.2f}")

    def connect(self) -> None:
        """
        Establish connection (no-op for simulated broker).

        The simulated broker doesn't need a real connection, but this
        method is required by the BaseBroker interface.
        """
        self._connected = True
        logger.debug("SimulatedBroker connected")

    def disconnect(self) -> None:
        """
        Disconnect (no-op for simulated broker).

        Cleans up any resources. Safe to call multiple times.
        """
        self._connected = False
        logger.debug("SimulatedBroker disconnected")

    def submit_order(self, order: OrderRequest) -> OrderResponse:
        """
        Submit an order to the simulated broker.

        Market orders are filled immediately at the current price with slippage.
        Limit and stop orders are held until their conditions are met.

        Args:
            order: Order request with all parameters

        Returns:
            Order response with execution details

        Raises:
            BrokerError: If no price data available for symbol
            InsufficientFundsError: If insufficient funds for purchase
            OrderRejectedError: If order is invalid
        """
        if self._current_candle is None:
            raise BrokerError("No price data available")

        order_id = f"bt_{uuid4()}"

        logger.debug(
            f"Submitting order {order_id}: {order.side.value} {order.quantity} "
            f"{order.symbol} @ {order.order_type.value}"
        )

        # Handle different order types
        if order.order_type == OrderType.MARKET:
            response = self._execute_market_order(order_id, order)
        elif order.order_type == OrderType.LIMIT:
            response = self._submit_limit_order(order_id, order)
        elif order.order_type == OrderType.STOP:
            response = self._submit_stop_order(order_id, order)
        else:
            raise OrderRejectedError(f"Order type {order.order_type} not supported")

        self._orders[order_id] = response
        return response

    def _execute_market_order(
        self, order_id: str, order: OrderRequest
    ) -> OrderResponse:
        """
        Execute a market order immediately with slippage.

        Args:
            order_id: Generated order ID
            order: Order request

        Returns:
            Completed order response

        Raises:
            InsufficientFundsError: If insufficient funds
        """
        fill_price = self._current_candle.close

        # Check buying power for purchases
        if order.side == OrderSide.BUY:
            required_cash = fill_price * order.quantity
            if required_cash > self._cash:
                raise InsufficientFundsError(
                    f"Insufficient funds: need ${required_cash:,.2f}, "
                    f"have ${self._cash:,.2f}"
                )

        # Check position for sells
        if order.side == OrderSide.SELL:
            total_assets = sum(order.filled_quantity for order in self._orders.values())
            if total_assets < order.quantity:
                raise OrderRejectedError(
                    f"Insufficient position: need {order.quantity} shares, "
                    f"have {total_assets}"
                )

        # Execute trade
        if order.side == OrderSide.BUY:
            self._cash -= fill_price * order.quantity
        else:
            self._cash += fill_price * order.quantity

        logger.info(
            f"Order {order_id} filled: {order.side.value} {order.quantity} "
            f"{order.symbol} @ ${fill_price:.2f}"
        )

        return OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=order.quantity,
            status=OrderStatus.FILLED,
            created_at=self._current_candle.timestamp,
            filled_at=self._current_candle.timestamp,
            avg_fill_price=fill_price,
        )

    def _submit_limit_order(self, order_id: str, order: OrderRequest) -> OrderResponse:
        """
        Submit a limit order (pending until price condition met).

        Args:
            order_id: Generated order ID
            order: Order request with limit_price

        Returns:
            Pending order response
        """
        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=OrderStatus.PENDING,
            created_at=self._current_candle.timestamp,
            avg_fill_price=None,
            limit_price=order.limit_price,
        )

        self._pending_orders[order_id] = (order, response)
        logger.debug(
            f"Limit order {order_id} pending: {order.side.value} @ ${order.limit_price:.2f}"
        )

        return response

    def _submit_stop_order(self, order_id: str, order: OrderRequest) -> OrderResponse:
        """
        Submit a stop order (pending until price condition met).

        Args:
            order_id: Generated order ID
            order: Order request with stop_price

        Returns:
            Pending order response
        """
        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=OrderStatus.PENDING,
            created_at=self._current_candle.timestamp,
            avg_fill_price=None,
            stop_price=order.stop_price,
        )

        self._pending_orders[order_id] = (order, response)
        logger.debug(
            f"Stop order {order_id} pending: {order.side.value} @ ${order.stop_price:.2f}"
        )

        return response

    def process_pending_orders(self) -> None:
        """Process pending limit and stop orders against current prices."""
        filled_orders = []

        for order_id, (order_req, order_resp) in self._pending_orders.items():
            current_price = self._current_candle.close
            should_fill = False

            # Check limit order conditions
            if order_req.order_type == OrderType.LIMIT:
                if (
                    order_req.side == OrderSide.BUY
                    and current_price <= order_req.limit_price
                ):
                    should_fill = True
                elif (
                    order_req.side == OrderSide.SELL
                    and current_price >= order_req.limit_price
                ):
                    should_fill = True

            # Check stop order conditions
            elif order_req.order_type == OrderType.STOP:
                if (
                    order_req.side == OrderSide.BUY
                    and current_price >= order_req.stop_price
                ):
                    should_fill = True
                elif (
                    order_req.side == OrderSide.SELL
                    and current_price <= order_req.stop_price
                ):
                    should_fill = True

            if should_fill:
                try:
                    fill_resp = self._execute_market_order(order_id, order_req)
                    self._orders[order_id] = fill_resp
                    filled_orders.append(order_id)
                except (InsufficientFundsError, OrderRejectedError) as e:
                    logger.warning(f"Pending order {order_id} rejected: {e}")
                    order_resp.status = OrderStatus.REJECTED
                    filled_orders.append(order_id)

        for order_id in filled_orders:
            self._pending_orders.pop(order_id)

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancelled, False if order not found or already filled
        """
        if order_id in self._pending_orders:
            _, response = self._pending_orders[order_id]
            response.status = OrderStatus.CANCELLED
            self._pending_orders.pop(order_id)
            logger.info(f"Order {order_id} cancelled")
            return True

        if order_id in self._orders and (order := self._orders[order_id]).status in {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.PENDING,
        }:
            order.status = OrderStatus.CANCELLED
            logger.info(f"Order {order_id} cancelled")
            return True

        return False

    def cancel_all_orders(self):
        for order in tuple(self._orders.values()):
            if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING}:
                self.cancel_order(order.order_id)

    def get_order(self, order_id: str) -> OrderResponse:
        """
        Get order details by ID.

        Args:
            order_id: Order ID

        Returns:
            Order response

        Raises:
            BrokerError: If order not found
        """
        if order_id in self._orders:
            return self._orders[order_id]
        if order_id in self._pending_orders:
            return self._pending_orders[order_id][1]
        raise BrokerError(f"Order {order_id} not found")

    def get_open_orders(self, symbol: str | None = None):
        """
        Get all open (pending) orders.

        Args:
            symbol: Optional symbol filter

        Returns: of open orders
        """
        if symbol:
            return [
                order
                for order in self._orders.values()
                if order.symbol == symbol
                and order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING}
            ]
        return [
            order
            for order in self._orders.values()
            if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING}
        ]

    def get_account(self) -> Account:
        """
        Get current account information.

        Returns:
            Account information including balances
        """
        equity = self._cash
        price = self._current_candle.close

        for order in self._orders.values():
            if order.side == OrderSide.SELL:
                continue

            diff = price - order.avg_fill_price
            equity += diff

        return Account(account_id=self._account_id, equity=equity, cash=self._cash)

    def get_historic_olhcv(
        self, symbol, timeframe, prev_bars=None, start_date=None, end_date=None
    ) -> list[OHLCV]:
        if start_date is not None and end_date is None:
            raise ValueError("End date must be provided if start date is provided")
        if start_date is None and end_date is not None:
            raise ValueError("Start date must be provided if end date is provided")
        
        with get_db_sess_sync() as db_sess:
            ...

    def yield_historic_ohlcv(
        self, symbol, timeframe, prev_bars=None, start_date=None, end_date=None
    ):
        if start_date is not None and end_date is None:
            raise ValueError("End date must be provided if start date is provided")
        if start_date is None and end_date is not None:
            raise ValueError("Start date must be provided if end date is provided")
        return super().yield_historic_ohlcv(
            symbol, timeframe, prev_bars, start_date, end_date
        )

    def yield_ohlcv(self, symbol, timeframe):
        return super().yield_ohlcv(symbol, timeframe)
