import asyncio
import logging

from alpaca.common.exceptions import APIError
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    OrderType as AlpacaOrderType,
    TimeInForce as AlpacaTimeInForce,
)
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    StopLimitOrderRequest,
    ReplaceOrderRequest,
)
from vegate.markets.schema import OHLC
from vegate.oms.enums import OrderSide, OrderType, OrderStatus
from vegate.oms.schema import Order, OrderRequest
from .base import BrokerClient
from .exception import BrokerClientException


# TODO: Implement async API
class AlpacaBrokerClient(BrokerClient):
    """Alpaca broker implementation using alpaca-py library."""

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
        oauth_token: str | None = None,
        paper: bool = True,
    ):
        """Initialize the Alpaca broker.

        Args:
            oauth_token: OAuth access token for Alpaca API
            paper: Whether to use paper trading (default: True)
        """
        super().__init__()
        self._api_key = api_key
        self._secret_key = secret_key
        self._oauth_token = oauth_token
        self._paper = paper
        self._logger = logging.getLogger(self.__class__.__name__)

        if (
            self._api_key is None
            and self._secret_key is None
            and self._oauth_token is None
        ):
            raise ValueError(
                "Either a combination of api_key and secret must "
                "be provided or oauth_token."
            )

        # Initialize the Alpaca trading client
        if self._oauth_token is not None:
            self.client = TradingClient(oauth_token=oauth_token, paper=self._paper)
        else:
            self.client = TradingClient(
                api_key=self._api_key, secret_key=self._secret_key, paper=self._paper
            )

        # Cache for orders
        self._orders: dict[str, Order] = {}

        self._candle_queue: asyncio.Queue[OHLC] | None = None

    def get_balance(self):
        return float(self.client.get_account().cash)

    def get_equity(self):
        return float(self.client.get_account().equity)
    
    def get_position(self, symbol: str) -> float:
        try:
            return float(self.client.get_open_position(symbol.replace("/", "")).qty)
        except APIError:
            return 0.0

    def place_order(self, request: OrderRequest) -> Order:
        """Place an order on Alpaca.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        try:
            # Map our OrderType to Alpaca OrderType
            alpaca_order_type = self._map_order_type(request.order_type)
            side = (
                AlpacaOrderSide.BUY
                if request.side == OrderSide.BUY
                else AlpacaOrderSide.SELL
            )
            # Create the appropriate Alpaca order request
            if alpaca_order_type == AlpacaOrderType.MARKET:
                alpaca_request = MarketOrderRequest(
                    symbol=request.symbol,
                    notional=request.notional,
                    qty=request.quantity,
                    side=side,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.LIMIT:
                alpaca_request = LimitOrderRequest(
                    symbol=request.symbol,
                    notional=request.notional,
                    qty=request.quantity,
                    side=side,
                    limit_price=request.limit_price,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP:
                alpaca_request = StopOrderRequest(
                    symbol=request.symbol,
                    notional=request.notional,
                    qty=request.quantity,
                    side=side,
                    stop_price=request.stop_price,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP_LIMIT:
                alpaca_request = StopLimitOrderRequest(
                    symbol=request.symbol,
                    qty=request.quantity,
                    notional=request.notional,
                    side=side,
                    limit_price=request.limit_price,
                    stop_price=request.stop_price,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            else:
                raise BrokerClientException(
                    f"Unsupported order type: {alpaca_order_type}"
                )

            # Submit the order to Alpaca
            alpaca_order = self.client.submit_order(alpaca_request)

            # Convert Alpaca order to our Order model
            order = self._convert_alpaca_order(alpaca_order)
            self._orders[order.id] = order

            self._logger.info(f"Order placed: {order.id}")
            return order

        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to place order: type: {type(e)} - {e}")
            raise

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
        try:
            # First, get the order to retrieve its details
            alpaca_order = self.client.get_order_by_id(order_id)

            # Use replace_order_by_id to modify the order
            modified_alpaca_order = self.client.replace_order_by_id(
                alpaca_order.id,
                ReplaceOrderRequest(
                    limit_price=limit_price or alpaca_order.limit_price,
                    stop_price=stop_price or alpaca_order.stop_price,
                ),
            )

            order = self._convert_alpaca_order(modified_alpaca_order)
            self._orders[order.id] = order

            self._logger.info(f"Order modified: {order_id}")
            return order
        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to modify order {order_id}: {e}")
            raise

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        try:
            self.client.cancel_order_by_id(order_id)

            # Update the order status in cache
            if order_id in self._orders:
                self._orders[order_id].status = OrderStatus.CANCELLED

            self._logger.info(f"Order cancelled: {order_id}")
            return True
        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully, False otherwise
        """
        try:
            # Get all orders first
            orders = self.get_orders()

            # Cancel each order individually
            all_cancelled = True
            for order in orders:
                if not self.cancel_order(order.id):
                    all_cancelled = False

            if all_cancelled:
                self._logger.info("All orders cancelled")
            else:
                self._logger.warning("Some orders failed to cancel")

            return all_cancelled
        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to cancel all orders: {e}")
            return False

    def get_order(self, order_id: str) -> Order | None:
        """Get a specific order.

        Args:
            order_id: ID of order to retrieve

        Returns:
            Order object or None if not found
        """
        try:
            alpaca_order = self.client.get_order_by_id(order_id)
            order = self._convert_alpaca_order(alpaca_order)
            self._orders[order.id] = order
            return order
        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to get order {order_id}: {e}")
            return None

    def get_orders(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of Order objects
        """
        try:
            alpaca_orders = self.client.get_orders()
            orders = [self._convert_alpaca_order(ao) for ao in alpaca_orders]
            for order in orders:
                self._orders[order.id] = order
            return orders
        except APIError as e:
            raise BrokerClientException(str(e))
        except BrokerClientException:
            raise
        except Exception as e:
            self._logger.error(f"Failed to get orders: {e}")
            return []

    def _map_order_type(self, order_type: OrderType) -> AlpacaOrderType:
        """Map our OrderType to Alpaca OrderType.

        Args:
            order_type: Our OrderType enum

        Returns:
            Alpaca OrderType enum
        """
        mapping = {
            OrderType.MARKET: AlpacaOrderType.MARKET,
            OrderType.LIMIT: AlpacaOrderType.LIMIT,
            OrderType.STOP: AlpacaOrderType.STOP,
            OrderType.STOP_LIMIT: AlpacaOrderType.STOP_LIMIT,
        }
        return mapping.get(order_type, AlpacaOrderType.MARKET)

    def _convert_alpaca_order(self, alpaca_order: AlpacaOrder) -> Order:
        """Convert an Alpaca order to our Order model.

        Args:
            alpaca_order: Alpaca order object

        Returns:
            Our Order model
        """
        return Order(
            id=str(alpaca_order.id),
            symbol=alpaca_order.symbol.replace("/", ""),
            quantity=float(alpaca_order.qty) if alpaca_order.qty else None,
            filled_quantity=float(alpaca_order.filled_qty),
            notional=float(alpaca_order.notional) if alpaca_order.notional else 0.0,
            order_type=self._map_alpaca_order_type(alpaca_order.type),
            side=(
                OrderSide.BUY
                if alpaca_order.side == AlpacaOrderSide.BUY
                else OrderSide.SELL
            ),
            limit_price=(
                float(alpaca_order.limit_price) if alpaca_order.limit_price else None
            ),
            stop_price=(
                float(alpaca_order.stop_price) if alpaca_order.stop_price else None
            ),
            filled_avg_price=(
                float(alpaca_order.filled_avg_price)
                if alpaca_order.filled_avg_price
                else None
            ),
            executed_at=alpaca_order.filled_at,
            submitted_at=alpaca_order.created_at,
            status=self._map_alpaca_order_status(alpaca_order.status),
        )

    def _map_alpaca_order_type(self, alpaca_order_type) -> OrderType:
        """Map Alpaca OrderType to our OrderType.

        Args:
            alpaca_order_type: Alpaca OrderType enum

        Returns:
            Our OrderType enum
        """
        mapping = {
            AlpacaOrderType.MARKET: OrderType.MARKET,
            AlpacaOrderType.LIMIT: OrderType.LIMIT,
            AlpacaOrderType.STOP: OrderType.STOP,
            AlpacaOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
        }
        return mapping.get(alpaca_order_type, OrderType.MARKET)

    def _map_alpaca_order_status(self, alpaca_status) -> OrderStatus:
        """Map Alpaca order status to our OrderStatus.

        Args:
            alpaca_status: Alpaca order status

        Returns:
            Our OrderStatus enum
        """
        status_str = str(alpaca_status).lower()
        if "pending" in status_str or "new" in status_str:
            return OrderStatus.PENDING
        elif "filled" in status_str:
            return OrderStatus.FILLED
        elif "cancelled" in status_str:
            return OrderStatus.CANCELLED
        else:
            return OrderStatus.PLACED
