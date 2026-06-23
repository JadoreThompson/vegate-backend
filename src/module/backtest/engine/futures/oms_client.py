import logging
import uuid

from module.broker.client.exception import BrokerClientException
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.oms.client import FuturesOMSClient
from vegate.oms.enums import OrderStatus, OrderType, PositionSide
from vegate.oms.schema import FuturesOrder, FuturesOrderRequest
from ..ohlc_feed_client import BacktestOHLCFeedClient


class FuturesBacktestOMSClient(FuturesOMSClient):
    """Futures OMS client implementation for backtesting."""

    def __init__(self, starting_balance: float):
        self.starting_balance = starting_balance
        self.equity = starting_balance
        self.balance = starting_balance
        self._order_map: dict[str, FuturesOrder] = {}
        self._pending_orders: list[FuturesOrder] = []
        self._positions: dict[str, dict] = {}
        self._leverage: dict[str, int] = {}
        self.ohlc_feed_client: BacktestOHLCFeedClient | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_balance(self) -> float:
        return self.balance

    def get_equity(self) -> float:
        self.equity = self._calculate_equity()
        return self.equity

    def get_position(self, symbol: str) -> float:
        pos = self._positions.get(symbol)
        if pos is None:
            return 0.0
        return pos["quantity"] * (
            1 if pos["position_side"] == PositionSide.LONG else -1
        )

    def get_positions(self) -> list[dict]:
        return list(self._positions.values())

    def set_leverage(self, symbol: str, leverage: int) -> None:
        self._leverage[symbol] = leverage

    def get_leverage(self, symbol: str) -> int:
        return self._leverage.get(symbol, 1)

    def place_order(self, request: FuturesOrderRequest) -> FuturesOrder:
        self._ensure_feed()

        if request.order_type == OrderType.LIMIT:
            return self._handle_limit_order(request)
        if request.order_type == OrderType.STOP:
            return self._handle_stop_order(request)
        if request.order_type == OrderType.MARKET:
            return self._handle_market_order(request)
        raise ValueError(f"Unsupported order type: {request.order_type}")

    def _handle_limit_order(self, request: FuturesOrderRequest) -> FuturesOrder:
        if request.limit_price is None or request.limit_price <= 0:
            raise ValueError("Limit price must be set and > 0 for limit orders")
        self._ensure_feed()
        cur_price = self.ohlc_feed_client.cur_candle.close

        if request.side == PositionSide.LONG:
            if request.limit_price >= cur_price:
                raise ValueError(
                    f"Long limit ({request.limit_price}) must be below current price ({cur_price})"
                )
        else:
            if request.limit_price <= cur_price:
                raise ValueError(
                    f"Short limit ({request.limit_price}) must be above current price ({cur_price})"
                )

        order = self._build_order(request, status=OrderStatus.PLACED)
        self._pending_orders.append(order)
        self._order_map[order.id] = order
        return order

    def _handle_stop_order(self, request: FuturesOrderRequest) -> FuturesOrder:
        if request.stop_price is None or request.stop_price <= 0:
            raise ValueError("Stop price must be set and > 0 for stop orders")
        self._ensure_feed()
        cur_price = self.ohlc_feed_client.cur_candle.close

        if request.side == PositionSide.LONG:
            if request.stop_price <= cur_price:
                raise ValueError(
                    f"Long stop ({request.stop_price}) must be above current price ({cur_price})"
                )
        else:
            if request.stop_price >= cur_price:
                raise ValueError(
                    f"Short stop ({request.stop_price}) must be below current price ({cur_price})"
                )

        order = self._build_order(request, status=OrderStatus.PLACED)
        self._pending_orders.append(order)
        self._order_map[order.id] = order
        return order

    def _handle_market_order(self, request: FuturesOrderRequest) -> FuturesOrder:
        self._ensure_feed()
        cur_price = self.ohlc_feed_client.cur_candle.close
        lev = self._leverage.get(request.symbol, 1)

        if request.notional is not None and request.notional > 0:
            order_cost = request.notional
        else:
            order_cost = (request.quantity or 0) * cur_price

        margin_req = order_cost / lev
        if self.balance < margin_req:
            order = self._build_order(request, status=OrderStatus.REJECTED)
            self._order_map[order.id] = order
            raise BrokerClientException(
                f"Insufficient margin: need {margin_req:.2f}, have {self.balance:.2f}"
            )

        order = self._build_order(
            request, status=OrderStatus.FILLED, fill_price=cur_price
        )
        self._order_map[order.id] = order
        self._apply_fill(order)
        return order

    def execute_pending_orders(self, candle: OHLCSchema) -> None:
        self._ensure_feed()
        high = candle.high
        low = candle.low

        for order in list(self._pending_orders):
            should_execute = False
            exec_price = None

            if order.order_type == OrderType.LIMIT:
                if order.side == PositionSide.LONG and low <= order.limit_price:
                    should_execute = True
                    exec_price = order.limit_price
                elif order.side == PositionSide.SHORT and high >= order.limit_price:
                    should_execute = True
                    exec_price = order.limit_price

            elif order.order_type == OrderType.STOP:
                if order.side == PositionSide.LONG and high >= order.stop_price:
                    should_execute = True
                    exec_price = order.stop_price
                elif order.side == PositionSide.SHORT and low <= order.stop_price:
                    should_execute = True
                    exec_price = order.stop_price

            if should_execute:
                lev = self._get_leverage_for_symbol(order.symbol)
                cost = (
                    order.notional
                    if order.notional
                    else (order.quantity or 0) * exec_price
                ) / lev
                if order.side == PositionSide.LONG and self.balance < cost:
                    order.status = OrderStatus.REJECTED
                    order.executed_at = candle.timestamp
                    self._pending_orders.remove(order)
                    continue

                order.status = OrderStatus.FILLED
                order.filled_quantity = order.quantity or 0
                order.avg_fill_price = exec_price
                order.executed_at = candle.timestamp
                self._apply_fill(order)
                self._pending_orders.remove(order)

    def _apply_fill(self, order: FuturesOrder) -> None:
        lev = self._get_leverage_for_symbol(order.symbol)
        cost = (
            order.notional
            if order.notional
            else (order.quantity or 0) * (order.avg_fill_price or 0)
        ) / lev

        pos = self._positions.setdefault(
            order.symbol,
            {
                "symbol": order.symbol,
                "quantity": 0.0,
                "position_side": order.side.value,
                "entry_price": 0.0,
            },
        )

        if order.side == PositionSide.LONG:
            total_qty = pos["quantity"] + order.filled_quantity
            if total_qty != 0:
                pos["entry_price"] = (
                    pos["entry_price"] * pos["quantity"]
                    + (order.avg_fill_price or 0) * order.filled_quantity
                ) / total_qty
            pos["quantity"] = total_qty
            self.balance -= cost
        else:
            total_qty = pos["quantity"] + order.filled_quantity
            if total_qty != 0:
                pos["entry_price"] = (
                    pos["entry_price"] * pos["quantity"]
                    + (order.avg_fill_price or 0) * order.filled_quantity
                ) / total_qty
            pos["quantity"] = total_qty
            self.balance -= cost

        if pos["quantity"] == 0:
            self._positions.pop(order.symbol, None)

    def modify_order(
        self,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
        take_profit: float | None = None,
        stop_loss: float | None = None,
    ) -> FuturesOrder:
        order = self._order_map.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        if limit_price is not None:
            order.limit_price = limit_price
        if stop_price is not None:
            order.stop_price = stop_price
        if take_profit is not None:
            order.take_profit = take_profit
        if stop_loss is not None:
            order.stop_loss = stop_loss
        return order

    def cancel_order(self, order_id: str) -> bool:
        order = self._order_map.get(order_id)
        if order and order.status == OrderStatus.PLACED:
            order.status = OrderStatus.CANCELLED
            if order in self._pending_orders:
                self._pending_orders.remove(order)
            return True
        return False

    def cancel_all_orders(self) -> bool:
        for order in list(self._order_map.values()):
            if order.status == OrderStatus.PLACED:
                order.status = OrderStatus.CANCELLED
        return True

    def get_order(self, order_id: str) -> FuturesOrder | None:
        return self._order_map.get(order_id)

    def get_orders(self) -> list[FuturesOrder]:
        return list(self._order_map.values())

    def _build_order(
        self,
        request: FuturesOrderRequest,
        status: OrderStatus,
        fill_price: float | None = None,
    ) -> FuturesOrder:
        self._ensure_feed()
        ts = self.ohlc_feed_client.cur_candle.timestamp
        order_id = str(uuid.uuid4())

        if fill_price is not None and status == OrderStatus.FILLED:
            qty = (
                request.quantity or (request.notional / fill_price)
                if request.notional
                else 0
            )
            filled = round(qty, 2)
            notional = round(filled * fill_price, 2)
            return FuturesOrder(
                id=order_id,
                symbol=request.symbol,
                contract_type=request.contract_type,
                side=request.side,
                quantity=request.quantity,
                filled_quantity=filled,
                notional=notional,
                order_type=request.order_type,
                side=request.side,
                limit_price=request.limit_price,
                stop_price=request.stop_price,
                take_profit=request.take_profit,
                stop_loss=request.stop_loss,
                avg_fill_price=fill_price,
                leverage=self._get_leverage_for_symbol(request.symbol),
                reduce_only=request.reduce_only,
                executed_at=ts,
                submitted_at=ts,
                status=status,
            )

        return FuturesOrder(
            id=order_id,
            symbol=request.symbol,
            contract_type=request.contract_type,
            side=request.side,
            quantity=request.quantity,
            filled_quantity=0.0,
            notional=request.notional,
            order_type=request.order_type,
            side=request.side,
            limit_price=request.limit_price,
            stop_price=request.stop_price,
            take_profit=request.take_profit,
            stop_loss=request.stop_loss,
            leverage=self._get_leverage_for_symbol(request.symbol),
            reduce_only=request.reduce_only,
            submitted_at=ts,
            status=status,
        )

    def _get_leverage_for_symbol(self, symbol: str) -> int:
        return self._leverage.get(symbol, 1)

    def _calculate_equity(self) -> float:
        if self.ohlc_feed_client is None or self.ohlc_feed_client.cur_candle is None:
            return self.balance
        cur_price = self.ohlc_feed_client.cur_candle.close
        unrealised = 0.0

        for pos in self._positions.values():
            if pos["quantity"] == 0:
                continue
            if pos["position_side"] == PositionSide.LONG.value:
                unrealised += pos["quantity"] * (cur_price - pos["entry_price"])
            else:
                unrealised += pos["quantity"] * (pos["entry_price"] - cur_price)

        return self.balance + unrealised

    def _ensure_feed(self) -> None:
        if self.ohlc_feed_client is None:
            raise ValueError(
                "OHLC feed is not set. OHLC feed is required to track current close price."
            )
