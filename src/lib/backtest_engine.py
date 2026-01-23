import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

from lib.brokers import BaseBroker
from models import (
    OHLC,
    BacktestMetrics,
    Order,
    OrderRequest,
)
from enums import OrderStatus, OrderType
from lib.strategy import BaseStrategy


logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for backtesting."""

    timeframe: str
    starting_balance: float
    symbol: str
    start_date: datetime
    end_date: datetime


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


class BacktestEngine:
    """Engine for running backtests on strategies."""

    def __init__(self, strategy_class: type[BaseStrategy], config: BacktestConfig):
        """Initialize the backtesting engine.

        Args:
            strategy_class: Strategy class to instantiate
            config: BacktestConfig object
        """
        self.strategy_class = strategy_class
        self.config = config
        self.broker = BacktestBroker(config.starting_balance)
        self.strategy = strategy_class(strategy_class.__name__, self.broker)
        self.candles: list[OHLC] = []

    def add_candles(self, candles: list[OHLC]) -> None:
        """Add candles to the backtest.

        Args:
            candles: List of OHLC candles
        """
        self.candles = sorted(candles, key=lambda c: c.timestamp)

    def run(self) -> BacktestMetrics:
        """Run the backtest.

        Returns:
            BacktestMetrics object with results
        """
        self.strategy.startup()

        # Filter candles by date range and symbol
        filtered_candles = [
            c
            for c in self.candles
            if c.symbol == self.config.symbol
            and self.config.start_date <= c.timestamp < self.config.end_date
            and c.timeframe == self.config.timeframe
        ]

        # Feed candles to strategy
        for candle in filtered_candles:
            self.strategy.on_candle(candle)

        self.strategy.shutdown()

        # Calculate metrics
        return self._calculate_metrics(filtered_candles)

    def _calculate_metrics(self, candles: list[OHLC]) -> BacktestMetrics:
        """Calculate backtest metrics.

        Args:
            candles: List of candles used in backtest

        Returns:
            BacktestMetrics object
        """
        orders = self.broker.get_orders()
        filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]

        # Calculate PnL
        total_buy_notional = sum(
            o.notional for o in self.broker.buy_orders if o.status == OrderStatus.FILLED
        )
        total_sell_notional = sum(
            abs(o.notional)
            for o in self.broker.sell_orders
            if o.status == OrderStatus.FILLED
        )

        total_pnl = total_sell_notional - total_buy_notional
        ending_balance = self.config.starting_balance + total_pnl

        # Calculate trade statistics
        num_trades = len(filled_orders)
        winning_trades = sum(1 for o in filled_orders if o.notional > 0)
        losing_trades = sum(1 for o in filled_orders if o.notional < 0)
        win_rate = (winning_trades / num_trades * 100) if num_trades > 0 else 0

        winning_notionals = [o.notional for o in filled_orders if o.notional > 0]
        losing_notionals = [abs(o.notional) for o in filled_orders if o.notional < 0]

        avg_win = (
            sum(winning_notionals) / len(winning_notionals) if winning_notionals else 0
        )
        avg_loss = (
            sum(losing_notionals) / len(losing_notionals) if losing_notionals else 0
        )

        profit_factor = (
            (sum(winning_notionals) / sum(losing_notionals))
            if losing_notionals and sum(losing_notionals) > 0
            else 0
        )

        total_return_percent = (
            (total_pnl / self.config.starting_balance * 100)
            if self.config.starting_balance > 0
            else 0
        )

        return BacktestMetrics(
            total_pnl=total_pnl,
            highest_balance=self.config.starting_balance
            + max((o.notional for o in filled_orders), default=0),
            lowest_balance=self.config.starting_balance
            + min((o.notional for o in filled_orders), default=0),
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbol=self.config.symbol,
            orders=orders,
            starting_balance=self.config.starting_balance,
            ending_balance=ending_balance,
            total_return_percent=total_return_percent,
            num_trades=num_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
        )
