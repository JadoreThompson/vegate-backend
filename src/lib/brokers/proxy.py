from collections.abc import AsyncGenerator, Generator
from uuid import UUID

from redis import Redis

from config import REDIS_ORDER_EVENTS_KEY
from events.order import OrderCancelled, OrderModified, OrderPlaced
from infra.redis.client import REDIS_CLIENT_SYNC
from models import OHLC, Order, OrderRequest
from enums import Timeframe
from .base import BaseBroker



class ProxyBroker(BaseBroker):
    """Proxy broker that wraps another broker and emits events for order operations."""

    def __init__(self, deployment_id: UUID, broker: BaseBroker,  redis_client: Redis = REDIS_CLIENT_SYNC):
        """Initialize the proxy broker.

        Args:
            deployment_id: ID of the deployment using this broker
            broker: The underlying broker to proxy calls to
        """
        self.deployment_id = deployment_id
        self.broker = broker
        self.supports_async = broker.supports_async
        self.redis_client = redis_client

    def get_balance(self):
        return self.broker.get_balance()
    
    def get_equity(self):
        return self.broker.get_equity()

    def place_order(self, order_request: OrderRequest) -> Order:
        """Place an order and emit OrderPlaced event.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        order = self.broker.place_order(order_request)
        event = OrderPlaced(deployment_id=self.deployment_id, order=order)
        # self.producer.send("orders", json.dumps(event.model_dump()).encode())
        self.redis_client.publish(REDIS_ORDER_EVENTS_KEY, event.model_dump_json())
        return order

    def modify_order(
        self,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        """Modify an order and emit OrderModified event.

        Args:
            order_id: ID of order to modify
            limit_price: New limit price (optional)
            stop_price: New stop price (optional)

        Returns:
            Modified Order object
        """
        order = self.broker.modify_order(order_id, limit_price, stop_price)
        event = OrderModified(deployment_id=self.deployment_id, order=order, success=True)
        # self.producer.send("orders", json.dumps(event.model_dump()).encode())
        self.redis_client.publish(REDIS_ORDER_EVENTS_KEY, event.model_dump_json())
        return order

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order and emit OrderCancelled event.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        success = self.broker.cancel_order(order_id)
        event = OrderCancelled(
            deployment_id=self.deployment_id, order_id=order_id, success=success
        )
        # self.producer.send("orders", json.dumps(event.model_dump()).encode())
        self.redis_client.publish(REDIS_ORDER_EVENTS_KEY, event.model_dump_json())
        return success

    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully, False otherwise
        """
        return self.broker.cancel_all_orders()

    def get_order(self, order_id: str) -> Order | None:
        """Get a specific order.

        Args:
            order_id: ID of order to retrieve

        Returns:
            Order object or None if not found
        """
        return self.broker.get_order(order_id)

    def get_orders(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of Order objects
        """
        return self.broker.get_orders()

    def stream_candles(
        self, symbol: str, timeframe: Timeframe
    ) -> Generator[OHLC, None, None]:
        """Stream candles synchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        return self.broker.stream_candles(symbol, timeframe)

    async def stream_candles_async(
        self, symbol: str, timeframe: Timeframe
    ) -> AsyncGenerator[OHLC, None]:
        """Stream candles asynchronously.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe

        Yields:
            OHLC candles
        """
        async for candle in self.broker.stream_candles_async(symbol, timeframe):
            yield candle
