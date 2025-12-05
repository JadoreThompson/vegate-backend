from typing import Optional, List
from datetime import datetime
import logging

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
)
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    TimeInForce as AlpacaTimeInForce,
    OrderType as AlpacaOrderType,
    OrderStatus as AlpacaOrderStatus,
)
from alpaca.common.exceptions import APIError

from .base import BaseBroker
from .rate_limiter import TokenBucketRateLimiter
from .exc import (
    BrokerError,
    AuthenticationError,
    OrderRejectedError,
    InsufficientFundsError,
    RateLimitError,
    ConnectionError as BrokerConnectionError,
)
from ..models import (
    OrderRequest,
    OrderResponse,
    Position,
    Account,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
)

logger = logging.getLogger(__name__)


class AlpacaBroker(BaseBroker):
    """
    Alpaca Markets broker implementation.

    Implements the BaseBroker interface using the alpaca-py library.
    Supports both paper and live trading through Alpaca's API.

    Attributes:
        api_key: Alpaca API key
        secret_key: Alpaca secret key
        paper: Whether to use paper trading (default: True)
        client: Alpaca trading client instance

    Example:
        broker = AlpacaBroker(
            api_key='your_api_key',
            secret_key='your_secret_key',
            paper=True
        )

        with broker:
            order = OrderRequest(
                symbol='AAPL',
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10
            )
            response = broker.submit_order(order)
    """

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        paper: bool = True,
        rate_limiter: Optional[TokenBucketRateLimiter] = None,
    ):
        """
        Initialize Alpaca broker.

        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
            paper: Use paper trading (default: True)
            rate_limiter: Optional rate limiter (default: 200 requests/minute)
        """
        # Initialize with default rate limiter if none provided
        # Alpaca allows 200 requests per minute
        if rate_limiter is None:
            rate_limiter = TokenBucketRateLimiter(rate=200, per_seconds=60)

        super().__init__(rate_limiter)

        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.client: Optional[TradingClient] = None

    def connect(self) -> None:
        """
        Establish connection to Alpaca and authenticate.

        Raises:
            AuthenticationError: If credentials are invalid
            BrokerConnectionError: If connection fails
        """
        try:
            self._apply_rate_limit()

            self.client = TradingClient(
                api_key=self.api_key, secret_key=self.secret_key, paper=self.paper
            )

            # Test connection by fetching account
            self.client.get_account()

            self._connected = True
            logger.info(
                f"Connected to Alpaca ({'paper' if self.paper else 'live'} trading)"
            )

        except APIError as e:
            if e.status_code == 401:
                raise AuthenticationError(
                    "Invalid Alpaca credentials", broker_code=str(e.status_code)
                ) from e
            else:
                raise BrokerConnectionError(
                    f"Failed to connect to Alpaca: {e}", broker_code=str(e.status_code)
                ) from e
        except Exception as e:
            raise BrokerConnectionError(
                f"Unexpected error connecting to Alpaca: {e}"
            ) from e

    def disconnect(self) -> None:
        """
        Disconnect from Alpaca.

        Alpaca's REST client doesn't maintain persistent connections,
        so this mainly cleans up the client reference.
        """
        self.client = None
        self._connected = False
        logger.info("Disconnected from Alpaca")

    def submit_order(self, order: OrderRequest) -> OrderResponse:
        """
        Submit an order to Alpaca.

        Args:
            order: Order request with all parameters

        Returns:
            Order response with submission details

        Raises:
            OrderRejectedError: If order is rejected
            InsufficientFundsError: If insufficient buying power
            RateLimitError: If rate limit exceeded
            BrokerError: For other submission errors
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            # Convert our order to Alpaca format
            alpaca_order = self._convert_order_to_alpaca(order)

            # Submit order
            alpaca_response = self.client.submit_order(alpaca_order)

            # Convert response to our format
            response = self._convert_order_from_alpaca(alpaca_response)

            logger.info(f"Submitted order: {response.order_id} for {order.symbol}")
            return response

        except APIError as e:
            self._handle_api_error(e, "submit_order")
        except Exception as e:
            self._log_error("submit_order", e)
            raise BrokerError(f"Failed to submit order: {e}") from e

    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an existing order.

        Args:
            order_id: Alpaca order ID

        Returns:
            True if cancelled successfully

        Raises:
            BrokerError: If cancellation fails
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            self.client.cancel_order_by_id(order_id)
            logger.info(f"Cancelled order: {order_id}")
            return True

        except APIError as e:
            if e.status_code == 404:
                logger.warning(f"Order not found: {order_id}")
                return False
            self._handle_api_error(e, "cancel_order")
        except Exception as e:
            self._log_error("cancel_order", e)
            raise BrokerError(f"Failed to cancel order: {e}") from e

    def get_order(self, order_id: str) -> OrderResponse:
        """
        Get order status.

        Args:
            order_id: Alpaca order ID

        Returns:
            Current order details

        Raises:
            BrokerError: If order cannot be retrieved
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_order = self.client.get_order_by_id(order_id)
            return self._convert_order_from_alpaca(alpaca_order)

        except APIError as e:
            self._handle_api_error(e, "get_order")
        except Exception as e:
            self._log_error("get_order", e)
            raise BrokerError(f"Failed to get order: {e}") from e

    def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderResponse]:
        """
        Get all open orders.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of open orders

        Raises:
            BrokerError: If orders cannot be retrieved
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            request = GetOrdersRequest(
                status="open", symbols=[symbol] if symbol else None
            )

            alpaca_orders = self.client.get_orders(filter=request)

            return [self._convert_order_from_alpaca(order) for order in alpaca_orders]

        except APIError as e:
            self._handle_api_error(e, "get_open_orders")
        except Exception as e:
            self._log_error("get_open_orders", e)
            raise BrokerError(f"Failed to get open orders: {e}") from e

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position if exists, None otherwise

        Raises:
            BrokerError: If position data cannot be retrieved
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_position = self.client.get_open_position(symbol)
            return self._convert_position_from_alpaca(alpaca_position)

        except APIError as e:
            if e.status_code == 404:
                return None
            self._handle_api_error(e, "get_position")
        except Exception as e:
            self._log_error("get_position", e)
            raise BrokerError(f"Failed to get position: {e}") from e

    def get_all_positions(self) -> List[Position]:
        """
        Get all positions.

        Returns:
            List of all positions

        Raises:
            BrokerError: If positions cannot be retrieved
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_positions = self.client.get_all_positions()

            return [self._convert_position_from_alpaca(pos) for pos in alpaca_positions]

        except APIError as e:
            self._handle_api_error(e, "get_all_positions")
        except Exception as e:
            self._log_error("get_all_positions", e)
            raise BrokerError(f"Failed to get positions: {e}") from e

    def close_position(self, symbol: str) -> OrderResponse:
        """
        Close entire position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Order response for closing order

        Raises:
            BrokerError: If position cannot be closed
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_order = self.client.close_position(symbol)
            return self._convert_order_from_alpaca(alpaca_order)

        except APIError as e:
            self._handle_api_error(e, "close_position")
        except Exception as e:
            self._log_error("close_position", e)
            raise BrokerError(f"Failed to close position: {e}") from e

    def get_account(self) -> Account:
        """
        Get account information.

        Returns:
            Account details

        Raises:
            BrokerError: If account data cannot be retrieved
        """
        if not self._connected or not self.client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_account = self.client.get_account()

            return Account(
                account_id=alpaca_account.id,
                equity=float(alpaca_account.equity),
                cash=float(alpaca_account.cash),
                buying_power=float(alpaca_account.buying_power),
                portfolio_value=float(alpaca_account.portfolio_value),
                last_updated=datetime.now(),
            )

        except APIError as e:
            self._handle_api_error(e, "get_account")
        except Exception as e:
            self._log_error("get_account", e)
            raise BrokerError(f"Failed to get account: {e}") from e

    # Helper methods for converting between our models and Alpaca's

    def _convert_order_to_alpaca(self, order: OrderRequest):
        """Convert our OrderRequest to Alpaca order request."""
        # Convert side
        side = (
            AlpacaOrderSide.BUY if order.side == OrderSide.BUY else AlpacaOrderSide.SELL
        )

        # Convert time in force
        tif_map = {
            TimeInForce.DAY: AlpacaTimeInForce.DAY,
            TimeInForce.GTC: AlpacaTimeInForce.GTC,
            TimeInForce.IOC: AlpacaTimeInForce.IOC,
            TimeInForce.FOK: AlpacaTimeInForce.FOK,
        }
        time_in_force = tif_map.get(order.time_in_force, AlpacaTimeInForce.DAY)

        # Create appropriate order type
        if order.order_type == OrderType.MARKET:
            return MarketOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                extended_hours=order.extended_hours,
                client_order_id=order.client_order_id,
            )
        elif order.order_type == OrderType.LIMIT:
            return LimitOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                limit_price=order.limit_price,
                extended_hours=order.extended_hours,
                client_order_id=order.client_order_id,
            )
        elif order.order_type == OrderType.STOP:
            return StopOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                stop_price=order.stop_price,
                extended_hours=order.extended_hours,
                client_order_id=order.client_order_id,
            )
        elif order.order_type == OrderType.STOP_LIMIT:
            return StopLimitOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                limit_price=order.limit_price,
                stop_price=order.stop_price,
                extended_hours=order.extended_hours,
                client_order_id=order.client_order_id,
            )
        else:
            raise BrokerError(f"Unsupported order type: {order.order_type}")

    def _convert_order_from_alpaca(self, alpaca_order) -> OrderResponse:
        """Convert Alpaca order to our OrderResponse."""
        # Convert status
        status_map = {
            AlpacaOrderStatus.NEW: OrderStatus.SUBMITTED,
            AlpacaOrderStatus.ACCEPTED: OrderStatus.SUBMITTED,
            AlpacaOrderStatus.PENDING_NEW: OrderStatus.PENDING,
            AlpacaOrderStatus.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
            AlpacaOrderStatus.FILLED: OrderStatus.FILLED,
            AlpacaOrderStatus.CANCELED: OrderStatus.CANCELLED,
            AlpacaOrderStatus.EXPIRED: OrderStatus.EXPIRED,
            AlpacaOrderStatus.REJECTED: OrderStatus.REJECTED,
        }
        status = status_map.get(alpaca_order.status, OrderStatus.PENDING)

        # Convert side
        side = (
            OrderSide.BUY
            if alpaca_order.side == AlpacaOrderSide.BUY
            else OrderSide.SELL
        )

        # Convert order type
        type_map = {
            AlpacaOrderType.MARKET: OrderType.MARKET,
            AlpacaOrderType.LIMIT: OrderType.LIMIT,
            AlpacaOrderType.STOP: OrderType.STOP,
            AlpacaOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
            AlpacaOrderType.TRAILING_STOP: OrderType.TRAILING_STOP,
        }
        order_type = type_map.get(alpaca_order.type, OrderType.MARKET)

        return OrderResponse(
            order_id=str(alpaca_order.id),
            client_order_id=alpaca_order.client_order_id,
            symbol=alpaca_order.symbol,
            side=side,
            order_type=order_type,
            quantity=float(alpaca_order.qty),
            filled_quantity=float(alpaca_order.filled_qty or 0),
            status=status,
            submitted_at=alpaca_order.submitted_at,
            filled_at=alpaca_order.filled_at,
            average_fill_price=(
                float(alpaca_order.filled_avg_price)
                if alpaca_order.filled_avg_price
                else None
            ),
            broker_metadata={
                "alpaca_order_class": (
                    str(alpaca_order.order_class)
                    if hasattr(alpaca_order, "order_class")
                    else None
                ),
                "alpaca_time_in_force": str(alpaca_order.time_in_force),
            },
        )

    def _convert_position_from_alpaca(self, alpaca_position) -> Position:
        """Convert Alpaca position to our Position."""
        quantity = float(alpaca_position.qty)
        side = OrderSide.BUY if quantity > 0 else OrderSide.SELL

        return Position(
            symbol=alpaca_position.symbol,
            quantity=abs(quantity),
            average_entry_price=float(alpaca_position.avg_entry_price),
            current_price=float(alpaca_position.current_price),
            market_value=float(alpaca_position.market_value),
            unrealized_pnl=float(alpaca_position.unrealized_pl),
            unrealized_pnl_percent=float(alpaca_position.unrealized_plpc) * 100,
            cost_basis=float(alpaca_position.cost_basis),
            side=side,
        )

    def _handle_api_error(self, error: APIError, operation: str):
        """Handle Alpaca API errors and convert to our exceptions."""
        self._log_error(operation, error)

        if error.status_code == 403:
            if "insufficient" in str(error).lower():
                raise InsufficientFundsError(
                    f"Insufficient funds: {error}", broker_code=str(error.status_code)
                ) from error
            else:
                raise OrderRejectedError(
                    f"Order rejected: {error}", broker_code=str(error.status_code)
                ) from error
        elif error.status_code == 429:
            raise RateLimitError(
                f"Rate limit exceeded: {error}",
                broker_code=str(error.status_code),
                retry_after=60,
            ) from error
        elif error.status_code == 422:
            raise OrderRejectedError(
                f"Invalid order parameters: {error}", broker_code=str(error.status_code)
            ) from error
        else:
            raise BrokerError(
                f"Alpaca API error: {error}", broker_code=str(error.status_code)
            ) from error
