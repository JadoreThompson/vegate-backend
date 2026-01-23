import logging
from collections.abc import AsyncGenerator, Generator

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    OrderType as AlpacaOrderType,
    TimeInForce as AlpacaTimeInForce,
)
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    StopLimitOrderRequest,
)

from enums import OrderType, OrderStatus
from models import Order, OrderRequest, OHLC
from lib.brokers import BaseBroker


class AlpacaBroker(BaseBroker):
    """Alpaca broker implementation using alpaca-py library."""

    supports_async: bool = False

    def __init__(
        self,
        oauth_token: str,
        paper: bool = True,
        is_crypto: bool = False,
    ):
        """Initialize the Alpaca broker.

        Args:
            oauth_token: OAuth access token for Alpaca API
            paper: Whether to use paper trading (default: True)
            is_crypto: Whether to trade crypto (default: False)
        """
        self.oauth_token = oauth_token
        self.paper = paper
        self.is_crypto = is_crypto
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize the Alpaca trading client
        self.client = TradingClient(
            api_key=None,
            secret_key=None,
            oauth_token=oauth_token,
            paper=paper,
        )

        # Cache for orders
        self._orders: dict[str, Order] = {}

    def place_order(self, order_request: OrderRequest) -> Order:
        """Place an order on Alpaca.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        try:
            # Map our OrderType to Alpaca OrderType
            alpaca_order_type = self._map_order_type(order_request.order_type)

            # Create the appropriate Alpaca order request
            if alpaca_order_type == AlpacaOrderType.MARKET:
                alpaca_request = MarketOrderRequest(
                    symbol=order_request.symbol,
                    qty=order_request.quantity,
                    side=AlpacaOrderSide.BUY
                    if order_request.notional > 0
                    else AlpacaOrderSide.SELL,
                    time_in_force=AlpacaTimeInForce.DAY,
                )
            elif alpaca_order_type == AlpacaOrderType.LIMIT:
                alpaca_request = LimitOrderRequest(
                    symbol=order_request.symbol,
                    qty=order_request.quantity,
                    side=AlpacaOrderSide.BUY
                    if order_request.notional > 0
                    else AlpacaOrderSide.SELL,
                    limit_price=order_request.limit_price,
                    time_in_force=AlpacaTimeInForce.DAY,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP:
                alpaca_request = StopOrderRequest(
                    symbol=order_request.symbol,
                    qty=order_request.quantity,
                    side=AlpacaOrderSide.BUY
                    if order_request.notional > 0
                    else AlpacaOrderSide.SELL,
                    stop_price=order_request.stop_price,
                    time_in_force=AlpacaTimeInForce.DAY,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP_LIMIT:
                alpaca_request = StopLimitOrderRequest(
                    symbol=order_request.symbol,
                    qty=order_request.quantity,
                    side=AlpacaOrderSide.BUY
                    if order_request.notional > 0
                    else AlpacaOrderSide.SELL,
                    limit_price=order_request.limit_price,
                    stop_price=order_request.stop_price,
                    time_in_force=AlpacaTimeInForce.DAY,
                )
            else:
                raise ValueError(f"Unsupported order type: {alpaca_order_type}")

            # Submit the order to Alpaca
            alpaca_order = self.client.submit_order(alpaca_request)

            # Convert Alpaca order to our Order model
            order = self._convert_alpaca_order(alpaca_order)
            self._orders[order.order_id] = order

            self.logger.info(f"Order placed: {order.order_id}")
            return order

        except Exception as e:
            self.logger.error(f"Failed to place order: {e}")
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
                order_id=order_id,
                qty=alpaca_order.qty,
                limit_price=limit_price or alpaca_order.limit_price,
                stop_price=stop_price or alpaca_order.stop_price,
            )

            order = self._convert_alpaca_order(modified_alpaca_order)
            self._orders[order.order_id] = order

            self.logger.info(f"Order modified: {order_id}")
            return order

        except Exception as e:
            self.logger.error(f"Failed to modify order {order_id}: {e}")
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
            self.logger.info(f"Order cancelled: {order_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully, False otherwise
        """
        try:
            self.client.cancel_all_orders()
            self.logger.info("All orders cancelled")
            return True
        except Exception as e:
            self.logger.error(f"Failed to cancel all orders: {e}")
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
            self._orders[order.order_id] = order
            return order
        except Exception as e:
            self.logger.error(f"Failed to get order {order_id}: {e}")
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
                self._orders[order.order_id] = order
            return orders
        except Exception as e:
            self.logger.error(f"Failed to get orders: {e}")
            return []

    def stream_candles(
        self, symbol: str, timeframe: str
    ) -> Generator[OHLC, None, None]:
        """Stream candles synchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1m", "5m", "1h")

        Yields:
            OHLC candles
        """
        raise NotImplementedError(
            "Synchronous candle streaming is not supported. Use stream_candles_async instead."
        )

    async def stream_candles_async(
        self, symbol: str, timeframe: str
    ) -> AsyncGenerator[OHLC, None]:
        """Stream candles asynchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1m", "5m", "1h")

        Yields:
            OHLC candles
        """
        raise NotImplementedError(
            "Asynchronous candle streaming is not yet implemented for AlpacaBroker."
        )

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

    def _convert_alpaca_order(self, alpaca_order) -> Order:
        """Convert an Alpaca order to our Order model.

        Args:
            alpaca_order: Alpaca order object

        Returns:
            Our Order model
        """
        return Order(
            order_id=alpaca_order.id,
            symbol=alpaca_order.symbol,
            quantity=float(alpaca_order.qty),
            notional=float(alpaca_order.notional) if alpaca_order.notional else 0.0,
            order_type=self._map_alpaca_order_type(alpaca_order.order_type),
            price=float(alpaca_order.filled_avg_price)
            if alpaca_order.filled_avg_price
            else None,
            limit_price=float(alpaca_order.limit_price)
            if alpaca_order.limit_price
            else None,
            stop_price=float(alpaca_order.stop_price)
            if alpaca_order.stop_price
            else None,
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
