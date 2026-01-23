import logging
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select

from infra.db.models import Ticks
from engine.enums import BrokerType
from engine.models import (
    OrderRequest,
    OrderResponse,
    Account,
    OrderSide,
    OrderStatus,
    OrderType,
)
from engine.ohlcv import OHLCV
from infra.db import get_db_sess_sync
from utils import get_datetime
from .base import BaseBroker
from .exc import BrokerError, OrderRejectedError


class BacktestBroker(BaseBroker):
    def __init__(self, starting_balance: float):
        self._cash = starting_balance
        self._orders: list[OrderResponse] = []
        self._orders: dict[str, OrderResponse] = {}
        self._pending_orders: dict[str, tuple[OrderRequest, OrderResponse]] = {}
        self._current_candle: OHLCV | None = None
        self._account_id = f"ac_{uuid4()}"
        self._source = BrokerType.ALPACA.value
        self._logger = logging.getLogger(type(self).__name__)

    def connect(self) -> None:
        """
        Establish connection (no-op for simulated broker).

        The simulated broker doesn't need a real connection, but this
        method is required by the BaseBroker interface.
        """
        self._connected = True
        self._logger.debug("SimulatedBroker connected")

    def disconnect(self) -> None:
        """
        Disconnect (no-op for simulated broker).

        Cleans up any resources. Safe to call multiple times.
        """
        self._connected = False
        self._logger.debug("SimulatedBroker disconnected")

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

        order_id = str(uuid4())

        self._logger.debug(
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
        order_quantity = order.quantity
        status = OrderStatus.FILLED

        # Check buying power for purchases
        if order.side == OrderSide.BUY:
            if order.notional is not None:
                required_cash = order.notional
                order_quantity = order.notional // fill_price
            else:
                required_cash = fill_price * order_quantity

            if required_cash > self._cash:
                status = OrderStatus.REJECTED

        # Check position for sells
        elif order.side == OrderSide.SELL:
            total_assets = sum(
                order.filled_quantity
                for order in self._orders.values()
                if order.side == OrderSide.BUY
                and order.status in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}
            )
            if total_assets < order_quantity:
                status = OrderStatus.REJECTED

        if order.notional is not None:
            quantity = order.notional / fill_price
        else:
            quantity = order.quantity

        # Execute trade
        if status == OrderStatus.FILLED:
            if order.side == OrderSide.BUY:
                self._cash -= required_cash
            else:
                self._cash += fill_price * order_quantity

        term = "filled" if status == OrderStatus.FILLED else "rejected"

        self._logger.info(
            f"Order {order_id} {term}: {order.side.value} {quantity} "
            f"{order.symbol} @ ${fill_price:.2f} cash: {self._cash}"
        )

        return OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order_quantity,
            filled_quantity=order_quantity,
            status=status,
            submitted_at=self._current_candle.timestamp,
            filled_at=self._current_candle.timestamp,
            avg_fill_price=fill_price,
            limit_price=None,
            stop_price=None,
            time_in_force=order.time_in_force,
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
        status = OrderStatus.PENDING

        if (
            order.side == OrderSide.BUY
            and order.limit_price >= self._current_candle.close
        ) or (
            order.side == OrderSide.SELL
            and order.limit_price <= self._current_candle.close
        ):
            status = OrderStatus.REJECTED

        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=status,
            submitted_at=self._current_candle.timestamp,
            avg_fill_price=None,
            limit_price=order.limit_price,
            stop_price=None,
            time_in_force=order.time_in_force,
        )

        if status == OrderStatus.PENDING:
            self._pending_orders[order_id] = (order, response)
            term = "pending"
        else:
            term = "rejected"

        self._logger.debug(
            f"Limit order {order_id} {term}: {order.side.value} @ ${order.limit_price:.2f}"
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
        status = OrderStatus.PENDING

        if (
            order.side == OrderSide.BUY
            and order.stop_price <= self._current_candle.close
        ) or (
            order.side == OrderSide.SELL
            and order.stop_price >= self._current_candle.close
        ):
            status = OrderStatus.REJECTED

        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=status,
            submitted_at=self._current_candle.timestamp,
            avg_fill_price=None,
            stop_price=order.stop_price,
            limit_price=None,
            time_in_force=order.time_in_force,
        )

        if status == OrderStatus.PENDING:
            self._pending_orders[order_id] = (order, response)
            term = "pending"
        else:
            term = "rejected"

        self._logger.debug(
            f"Stop order {order_id} {term}: {order.side.value} @ ${order.stop_price:.2f}"
        )

        return response

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
            self._logger.info(f"Order {order_id} cancelled")
            return True

        if order_id in self._orders and (order := self._orders[order_id]).status in {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.PENDING,
        }:
            order.status = OrderStatus.CANCELLED
            self._logger.info(f"Order {order_id} cancelled")
            return True

        return False

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

        if self._current_candle is not None:
            price = self._current_candle.close
            qty = 0.0

            for order in self._orders.values():
                if order.status not in {
                    OrderStatus.PARTIALLY_FILLED,
                    OrderStatus.FILLED,
                }:
                    continue

                if order.side == OrderSide.SELL:
                    qty -= order.filled_quantity
                else:
                    qty += order.filled_quantity

            equity += price * qty

        return Account(account_id=self._account_id, equity=equity, cash=self._cash)

    def get_historic_ohlcv(
        self, symbol, timeframe, prev_bars=None, start_date=None, end_date=None
    ) -> list[OHLCV]:
        if start_date is None and end_date is not None:
            raise ValueError("Start date must be provided if end date is provided")

        stmt = (
            select(Ticks)
            .where(Ticks.source == self._source, Ticks.symbol == symbol)
            .order_by(Ticks.timestamp)
        )

        if prev_bars is not None:
            end_date = get_datetime()
            start_date = end_date - timedelta(
                seconds=timeframe.get_seconds() * (prev_bars + 1)
            )

        if start_date is not None:
            stmt = stmt.where(Ticks.timestamp >= start_date.timestamp())
        if end_date is not None:
            stmt = stmt.where(Ticks.timestamp <= end_date.timestamp())

        candles = []
        current_candle = None
        current_candle_ts = None
        tf_secs = timeframe.get_seconds()

        with get_db_sess_sync() as db_sess:
            res = db_sess.scalars(stmt)
            for tick in res:
                if current_candle is None:
                    current_candle_ts = int(tick.timestamp // tf_secs) * tf_secs
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(current_candle_ts),
                        open=tick.price,
                        high=tick.price,
                        low=tick.price,
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                    continue

                t = current_candle.timestamp

                if (next_start := t + tf_secs) <= tick.timestamp:
                    current_candle_ts = next_start
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(next_start),
                        open=current_candle.close,
                        high=max(current_candle.high, tick.price),
                        low=min(current_candle.low, tick.price),
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                    candles.append(current_candle)
                else:
                    current_candle.close = tick.price
                    current_candle.high = max(current_candle.close, current_candle.high)
                    current_candle.low = min(current_candle.close, current_candle.low)
                    current_candle.volume += tick.size

        if candles and current_candle is not None and candles[-1] is not current_candle:
            candles.append(current_candle)

        if prev_bars is not None:
            return candles[:prev_bars]

        return candles

    def yield_historic_ohlcv(
        self, symbol, timeframe, prev_bars=None, start_date=None, end_date=None
    ):
        if start_date is not None and end_date is None:
            raise ValueError("End date must be provided if start date is provided")
        if start_date is None and end_date is not None:
            raise ValueError("Start date must be provided if end date is provided")

        if start_date is not None:
            start_date = datetime(
                year=start_date.year, month=start_date.month, day=start_date.day
            )
            end_date = datetime(
                year=end_date.year, month=end_date.month, day=end_date.day
            )

        if prev_bars is not None:
            end_date = get_datetime()
            start_date = end_date - timedelta(
                seconds=timeframe.get_seconds() * (prev_bars + 1)
            )

        stmt = (
            select(Ticks)
            .where(Ticks.source == self._source, Ticks.symbol == symbol)
            .order_by(Ticks.timestamp)
        )

        if start_date is not None:
            stmt = stmt.where(Ticks.timestamp >= start_date.timestamp())
        if end_date is not None:
            stmt = stmt.where(Ticks.timestamp <= end_date.timestamp())

        current_candle = None
        current_candle_ts = None
        secs = timeframe.get_seconds()

        with get_db_sess_sync() as db_sess:
            res = db_sess.scalars(stmt)
            for tick in res:
                tick_ts = tick.timestamp
                candle_start_ts = (tick_ts // secs) * secs

                if current_candle is None:
                    current_candle_ts = candle_start_ts
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(candle_start_ts),
                        open=tick.price,
                        high=tick.price,
                        low=tick.price,
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                    continue

                if tick_ts >= current_candle_ts + secs:
                    self._current_candle = current_candle
                    yield current_candle
                    current_candle_ts = candle_start_ts
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(candle_start_ts),
                        open=current_candle.close,
                        high=tick.price,
                        low=tick.price,
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                else:
                    current_candle.close = tick.price
                    current_candle.high = max(current_candle.high, tick.price)
                    current_candle.low = min(current_candle.low, tick.price)
                    current_candle.volume += tick.size

        if current_candle is not None:
            self._current_candle = current_candle
            yield current_candle

    def yield_ohlcv(self, symbol, timeframe):
        tf_secs = timeframe.get_seconds()
        current_candle = None
        current_candle_ts = None

        stmt = (
            select(Ticks)
            .where(Ticks.source == self._source, Ticks.symbol == symbol)
            .order_by(Ticks.timestamp)
        )

        with get_db_sess_sync() as db_sess:
            res = db_sess.scalars(stmt)

            for tick in res.yield_per(1000):
                tick_ts = tick.timestamp
                candle_start_ts = (tick_ts // tf_secs) * tf_secs

                if current_candle is None:
                    current_candle_ts = candle_start_ts
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(candle_start_ts),
                        open=tick.price,
                        high=tick.price,
                        low=tick.price,
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                    self._current_candle = current_candle
                    continue

                if tick_ts >= current_candle_ts + tf_secs:
                    self._current_candle = current_candle
                    yield current_candle

                    current_candle_ts = candle_start_ts
                    current_candle = OHLCV(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(candle_start_ts),
                        open=current_candle.close,
                        high=tick.price,
                        low=tick.price,
                        close=tick.price,
                        volume=tick.size,
                        timeframe=timeframe,
                    )
                    self._current_candle = current_candle
                else:
                    current_candle.close = tick.price
                    current_candle.high = max(current_candle.high, tick.price)
                    current_candle.low = min(current_candle.low, tick.price)
                    current_candle.volume += tick.size
                    self._current_candle = current_candle

        if current_candle is not None:
            self._current_candle = current_candle
            yield current_candle

    def process_pending_orders(self) -> None:
        """Process pending limit and stop orders against current prices."""
        filled_orders = []

        for order_id, (order_req, _) in self._pending_orders.items():
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
                fill_resp = self._execute_market_order(order_id, order_req)
                self._orders[order_id] = fill_resp
                filled_orders.append(order_id)

        for order_id in filled_orders:
            self._pending_orders.pop(order_id)

    def cancel_all_orders(self):
        for order in tuple(self._orders.values()):
            if order.status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING}:
                self.cancel_order(order.order_id)
