import logging
from typing import Optional, List, Dict
from datetime import datetime
import uuid

from ..brokers.base import BaseBroker
from ..brokers.exc import BrokerError, InsufficientFundsError, OrderRejectedError
from ..models import (
    OrderRequest,
    OrderResponse,
    Position,
    Account,
    OrderSide,
    OrderStatus,
    OrderType,
)

logger = logging.getLogger(__name__)


class SimulatedBroker(BaseBroker):
    """
    Simulated broker for backtesting that implements the BaseBroker interface.

    This broker simulates order execution using historical data with configurable
    slippage and commission structures. It maintains portfolio state including
    cash, positions, and tracks all executed orders.

    Features:
        - Realistic order fills with slippage
        - Configurable commission structure
        - Portfolio tracking (cash, positions, equity)
        - Market order execution using current bar close price
        - Limit/stop order execution with proper fill logic
        - Order validation against available cash and positions

    Example:
        broker = SimulatedBroker(
            initial_capital=100000.0,
            commission_per_share=0.0,
            commission_percent=0.1,
            slippage_percent=0.1
        )
        broker.connect()

        # Set current market state
        broker.set_current_time(datetime.now())
        broker.set_current_price("AAPL", 150.0)

        # Submit order
        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10
        )
        response = broker.submit_order(order)
    """

    def __init__(
        self,
        initial_capital: float,
        commission_per_share: float = 0.0,
        commission_percent: float = 0.0,
        slippage_percent: float = 0.1,
    ):
        """
        Initialize simulated broker.

        Args:
            initial_capital: Starting cash balance
            commission_per_share: Fixed commission per share (default: 0.0)
            commission_percent: Commission as percentage of trade value (default: 0.0)
            slippage_percent: Slippage as percentage of price (default: 0.1%)
        """
        super().__init__(rate_limiter=None)  # No rate limiting needed for simulation

        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.commission_per_share = commission_per_share
        self.commission_percent = commission_percent
        self.slippage_percent = slippage_percent

        # Portfolio state
        self.positions: Dict[str, Position] = {}
        self.orders: Dict[str, OrderResponse] = {}
        self.pending_orders: Dict[str, tuple[OrderRequest, OrderResponse]] = {}

        # Market state
        self.current_time: Optional[datetime] = None
        self.current_prices: Dict[str, float] = {}

        # Order tracking
        self._order_counter = 0

        logger.info(
            f"SimulatedBroker initialized: capital=${initial_capital:,.2f}, "
            f"commission={commission_percent}%, slippage={slippage_percent}%"
        )

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

    def set_current_time(self, timestamp: datetime) -> None:
        """
        Update simulation time.

        Args:
            timestamp: Current timestamp in the simulation
        """
        self.current_time = timestamp

        # Process any pending limit/stop orders
        self._process_pending_orders()

    def set_current_price(self, symbol: str, price: float) -> None:
        """
        Update current market price for a symbol.

        Args:
            symbol: Trading symbol
            price: Current market price
        """
        self.current_prices[symbol] = price

        # Update position market values
        if symbol in self.positions:
            pos = self.positions[symbol]
            pos.current_price = price
            pos.market_value = pos.quantity * price
            pos.unrealized_pnl = pos.market_value - pos.cost_basis
            pos.unrealized_pnl_percent = (
                (pos.unrealized_pnl / pos.cost_basis * 100)
                if pos.cost_basis != 0
                else 0.0
            )

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
        # Validate symbol has price data
        if order.symbol not in self.current_prices:
            raise BrokerError(f"No price data available for {order.symbol}")

        # Validate order
        if order.quantity <= 0:
            raise OrderRejectedError("Order quantity must be positive")

        # Generate order ID
        self._order_counter += 1
        order_id = f"SIM{self._order_counter:08d}"

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

        self.orders[order_id] = response
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
        base_price = self.current_prices[order.symbol]

        # Calculate fill price with slippage
        slippage_factor = 1 + (self.slippage_percent / 100)
        if order.side == OrderSide.BUY:
            fill_price = base_price * slippage_factor
        else:
            fill_price = base_price / slippage_factor

        # Calculate commission
        commission = self._calculate_commission(order.quantity, fill_price)

        # Check buying power for purchases
        if order.side == OrderSide.BUY:
            required_cash = fill_price * order.quantity + commission
            if required_cash > self.cash:
                raise InsufficientFundsError(
                    f"Insufficient funds: need ${required_cash:,.2f}, "
                    f"have ${self.cash:,.2f}"
                )

        # Check position for sells
        if order.side == OrderSide.SELL:
            position = self.positions.get(order.symbol)
            if not position or position.quantity < order.quantity:
                current_qty = position.quantity if position else 0
                raise OrderRejectedError(
                    f"Insufficient position: need {order.quantity} shares, "
                    f"have {current_qty}"
                )

        # Execute trade
        if order.side == OrderSide.BUY:
            self.cash -= fill_price * order.quantity + commission
            self._update_position(order.symbol, order.quantity, fill_price)
        else:
            self.cash += fill_price * order.quantity - commission
            self._update_position(order.symbol, -order.quantity, fill_price)

        slippage_cost = abs(fill_price - base_price) * order.quantity

        logger.info(
            f"Order {order_id} filled: {order.side.value} {order.quantity} "
            f"{order.symbol} @ ${fill_price:.2f} "
            f"(commission=${commission:.2f}, slippage=${slippage_cost:.2f})"
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
            submitted_at=self.current_time,
            filled_at=self.current_time,
            average_fill_price=fill_price,
            broker_metadata={
                "commission": commission,
                "slippage": fill_price - base_price,
                "base_price": base_price,
            },
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
        if order.limit_price is None:
            raise OrderRejectedError("Limit orders require limit_price")

        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=OrderStatus.PENDING,
            submitted_at=self.current_time,
            average_fill_price=None,
            broker_metadata={"limit_price": order.limit_price},
        )

        self.pending_orders[order_id] = (order, response)
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
        if order.stop_price is None:
            raise OrderRejectedError("Stop orders require stop_price")

        response = OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=0.0,
            status=OrderStatus.PENDING,
            submitted_at=self.current_time,
            average_fill_price=None,
            broker_metadata={"stop_price": order.stop_price},
        )

        self.pending_orders[order_id] = (order, response)
        logger.debug(
            f"Stop order {order_id} pending: {order.side.value} @ ${order.stop_price:.2f}"
        )

        return response

    def _process_pending_orders(self) -> None:
        """Process pending limit and stop orders against current prices."""
        filled_orders = []

        for order_id, (order_req, order_resp) in self.pending_orders.items():
            symbol = order_req.symbol
            if symbol not in self.current_prices:
                continue

            current_price = self.current_prices[symbol]
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
                    # Convert to market order and execute
                    market_order = OrderRequest(
                        symbol=order_req.symbol,
                        side=order_req.side,
                        order_type=OrderType.MARKET,
                        quantity=order_req.quantity,
                        client_order_id=order_req.client_order_id,
                    )
                    # Execute without creating new order ID
                    fill_resp = self._execute_market_order_sync(order_id, market_order)
                    self.orders[order_id] = fill_resp
                    filled_orders.append(order_id)
                except (InsufficientFundsError, OrderRejectedError) as e:
                    logger.warning(f"Pending order {order_id} rejected: {e}")
                    order_resp.status = OrderStatus.REJECTED
                    filled_orders.append(order_id)

        # Remove filled orders from pending
        for order_id in filled_orders:
            del self.pending_orders[order_id]

    def _execute_market_order_sync(
        self, order_id: str, order: OrderRequest
    ) -> OrderResponse:
        """Synchronous version of market order execution for internal use."""
        base_price = self.current_prices[order.symbol]
        slippage_factor = 1 + (self.slippage_percent / 100)

        if order.side == OrderSide.BUY:
            fill_price = base_price * slippage_factor
        else:
            fill_price = base_price / slippage_factor

        commission = self._calculate_commission(order.quantity, fill_price)

        if order.side == OrderSide.BUY:
            required_cash = fill_price * order.quantity + commission
            if required_cash > self.cash:
                raise InsufficientFundsError(
                    f"Insufficient funds: need ${required_cash:,.2f}"
                )
            self.cash -= fill_price * order.quantity + commission
            self._update_position(order.symbol, order.quantity, fill_price)
        else:
            position = self.positions.get(order.symbol)
            if not position or position.quantity < order.quantity:
                raise OrderRejectedError("Insufficient position")
            self.cash += fill_price * order.quantity - commission
            self._update_position(order.symbol, -order.quantity, fill_price)

        return OrderResponse(
            order_id=order_id,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=order.quantity,
            status=OrderStatus.FILLED,
            submitted_at=self.current_time,
            filled_at=self.current_time,
            average_fill_price=fill_price,
            broker_metadata={"commission": commission},
        )

    def _calculate_commission(self, quantity: float, price: float) -> float:
        """
        Calculate total commission for a trade.

        Args:
            quantity: Number of shares
            price: Price per share

        Returns:
            Total commission amount
        """
        per_share_comm = quantity * self.commission_per_share
        percent_comm = price * quantity * (self.commission_percent / 100)
        return per_share_comm + percent_comm

    def _update_position(
        self, symbol: str, quantity_delta: float, price: float
    ) -> None:
        """
        Update position after trade execution.

        Args:
            symbol: Trading symbol
            quantity_delta: Change in quantity (positive for buy, negative for sell)
            price: Execution price
        """
        if symbol in self.positions:
            pos = self.positions[symbol]
            new_quantity = pos.quantity + quantity_delta

            if abs(new_quantity) < 1e-6:  # Close to zero, close position
                del self.positions[symbol]
                logger.debug(f"Position closed: {symbol}")
            else:
                # Update average entry price for additions to position
                if (quantity_delta > 0 and pos.quantity > 0) or (
                    quantity_delta < 0 and pos.quantity < 0
                ):
                    new_cost_basis = pos.cost_basis + (quantity_delta * price)
                    pos.cost_basis = new_cost_basis
                    pos.average_entry_price = new_cost_basis / new_quantity
                else:
                    # Reducing or flipping position
                    pos.cost_basis = pos.cost_basis + (quantity_delta * price)

                pos.quantity = new_quantity
                pos.market_value = new_quantity * self.current_prices.get(symbol, price)
                pos.unrealized_pnl = pos.market_value - pos.cost_basis
                pos.unrealized_pnl_percent = (
                    (pos.unrealized_pnl / abs(pos.cost_basis) * 100)
                    if pos.cost_basis != 0
                    else 0.0
                )
                pos.side = OrderSide.BUY if new_quantity > 0 else OrderSide.SELL
        else:
            if abs(quantity_delta) > 1e-6:  # Only create position if non-zero
                self.positions[symbol] = Position(
                    symbol=symbol,
                    quantity=quantity_delta,
                    average_entry_price=price,
                    current_price=self.current_prices.get(symbol, price),
                    market_value=quantity_delta
                    * self.current_prices.get(symbol, price),
                    unrealized_pnl=0.0,
                    unrealized_pnl_percent=0.0,
                    cost_basis=quantity_delta * price,
                    side=OrderSide.BUY if quantity_delta > 0 else OrderSide.SELL,
                )
                logger.debug(f"Position opened: {symbol} qty={quantity_delta}")

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancelled, False if order not found or already filled
        """
        if order_id in self.pending_orders:
            _, response = self.pending_orders[order_id]
            response.status = OrderStatus.CANCELLED
            del self.pending_orders[order_id]
            logger.info(f"Order {order_id} cancelled")
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
        if order_id in self.orders:
            return self.orders[order_id]
        elif order_id in self.pending_orders:
            return self.pending_orders[order_id][1]
        else:
            raise BrokerError(f"Order {order_id} not found")

    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderResponse]:
        """
        Get all open (pending) orders.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of open orders
        """
        orders = [resp for _, resp in self.pending_orders.values()]
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get current position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position if exists, None otherwise
        """
        return self.positions.get(symbol)

    def get_all_positions(self) -> List[Position]:
        """
        Get all current positions.

        Returns:
            List of all positions
        """
        return list(self.positions.values())

    def close_position(self, symbol: str) -> OrderResponse:
        """
        Close entire position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Order response for closing order

        Raises:
            BrokerError: If no position exists
        """
        position = self.positions.get(symbol)
        if not position:
            raise BrokerError(f"No position exists for {symbol}")

        # Create market order to close position
        close_order = OrderRequest(
            symbol=symbol,
            side=OrderSide.SELL if position.quantity > 0 else OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=abs(position.quantity),
        )

        return self.submit_order(close_order)

    def get_account(self) -> Account:
        """
        Get current account information.

        Returns:
            Account information including balances
        """
        portfolio_value = self.cash + sum(
            pos.market_value for pos in self.positions.values()
        )

        return Account(
            account_id="SIMULATED",
            equity=portfolio_value,
            cash=self.cash,
            buying_power=self.cash,
            portfolio_value=portfolio_value,
            last_updated=self.current_time or datetime.utcnow(),
        )
