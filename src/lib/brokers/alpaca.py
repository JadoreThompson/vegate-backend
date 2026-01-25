import asyncio
import json
import logging
import websockets
from collections.abc import AsyncGenerator, Generator
from datetime import datetime, timedelta

from alpaca.data.models import Bar
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
    ReplaceOrderRequest
)

from enums import OrderSide, OrderType, OrderStatus, Timeframe
from models import Order, OrderRequest, OHLC
from .base import BaseBroker


class AlpacaBroker(BaseBroker):
    """Alpaca broker implementation using alpaca-py library."""

    supports_async: bool = True

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
            self.client = TradingClient(oauth_token=oauth_token, paper=paper)
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
            side = (
                AlpacaOrderSide.BUY
                if order_request.side == OrderSide.BUY
                else AlpacaOrderSide.SELL
            )
            # Create the appropriate Alpaca order request
            if alpaca_order_type == AlpacaOrderType.MARKET:
                alpaca_request = MarketOrderRequest(
                    symbol=order_request.symbol,
                    notional=order_request.notional,
                    qty=order_request.quantity,
                    side=side,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.LIMIT:
                alpaca_request = LimitOrderRequest(
                    symbol=order_request.symbol,
                    notional=order_request.notional,
                    qty=order_request.quantity,
                    side=side,
                    limit_price=order_request.limit_price,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP:
                alpaca_request = StopOrderRequest(
                    symbol=order_request.symbol,
                    notional=order_request.notional,
                    qty=order_request.quantity,
                    side=side,
                    stop_price=order_request.stop_price,
                    time_in_force=AlpacaTimeInForce.GTC,
                )
            elif alpaca_order_type == AlpacaOrderType.STOP_LIMIT:
                alpaca_request = StopLimitOrderRequest(
                    symbol=order_request.symbol,
                    qty=order_request.quantity,
                    side=(
                        AlpacaOrderSide.BUY
                        if order_request.notional > 0
                        else AlpacaOrderSide.SELL
                    ),
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

            self._logger.info(f"Order placed: {order.order_id}")
            return order

        except Exception as e:
            self._logger.error(f"Failed to place order: {e}")
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
                )
            )

            order = self._convert_alpaca_order(modified_alpaca_order)
            self._orders[order.order_id] = order

            self._logger.info(f"Order modified: {order_id}")
            return order

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
                if not self.cancel_order(order.order_id):
                    all_cancelled = False

            if all_cancelled:
                self._logger.info("All orders cancelled")
            else:
                self._logger.warning("Some orders failed to cancel")

            return all_cancelled
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
            self._orders[order.order_id] = order
            return order
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
                self._orders[order.order_id] = order
            return orders
        except Exception as e:
            self._logger.error(f"Failed to get orders: {e}")
            return []

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
        raise NotImplementedError(
            "Synchronous candle streaming is not supported. Use stream_candles_async instead."
        )

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
        task = asyncio.create_task(self._handle_stream_alpaca(symbol, timeframe))
        self._candle_queue = asyncio.Queue()

        try:
            while True:
                item = await self._candle_queue.get()
                yield item
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _handle_stream_alpaca(self, symbol: str, timeframe: Timeframe) -> None:
        print(1)
        async with websockets.connect(
            "wss://stream.data.alpaca.markets/v2/iex"
            # "wss://stream.data.alpaca.markets/v1beta3/crypto/eu-1"
        ) as ws:
            auth_msg = {
                "action": "auth",
                "key": self._api_key,
                "secret": self._secret_key,
            }

            await ws.send(json.dumps(auth_msg))

            resp = await ws.recv()
            self._logger.info(f"Auth response: {resp}")

            # Determine if we should subscribe to minute or daily bars
            timeframe_seconds = timeframe.get_seconds()
            is_intraday = timeframe_seconds < 86400  # 1 day in seconds

            sub_msg = {"action": "subscribe"}

            if is_intraday:
                sub_msg["bars"] = [symbol]
                handler = lambda bar: self._on_minute_bar(bar, timeframe_seconds)
            else:
                sub_msg["dailyBars"] = [symbol]
                handler = lambda bar: self._on_daily_bar(bar)

            await ws.send(json.dumps(sub_msg))
            resp = await ws.recv()
            self._logger.info(f"Subscribe response: {resp}")

            self._aggregation_state = {
                "symbol": symbol,
                "timeframe": timeframe,
                "timeframe_seconds": timeframe_seconds,
                "current_candle": None,
                "is_intraday": is_intraday,
            }

            # Skipping the first message which is usually a confirmation
            message = await ws.recv()

            while True:
                message = await ws.recv()
                data = json.loads(message)
                handler(data[0])

    def _on_minute_bar(self, bar: dict, timeframe_seconds: int) -> None:
        """Handle minute bar updates for intraday aggregation.

        Args:
            bar: Bar object from Alpaca stream
            timeframe_seconds: The timeframe in seconds
        """
        if not hasattr(self, "_aggregation_state"):
            return

        state = self._aggregation_state

        # Convert bar timestamp to the candle period
        bar_start = datetime.fromisoformat(bar["t"])
        period_start = self._get_period_start(bar_start, timeframe_seconds)

        if state["current_candle"] is None:
            if period_start != bar_start:
                return  # Not the start of a new candle yet

            state["current_candle"] = {
                "open": bar["o"],
                "high": bar["h"],
                "low": bar["l"],
                "close": bar["c"],
                "volume": bar["v"],
                "period_start": period_start,
                "period_end": period_start + timedelta(seconds=timeframe_seconds),
            }

        bar_end = bar_start + timedelta(seconds=timeframe_seconds)
        cur_candle = state["current_candle"]
        period_end = cur_candle["period_end"]

        if bar_end <= period_end:
            cur_candle["high"] = max(cur_candle["high"], bar["h"])
            cur_candle["low"] = min(cur_candle["low"], bar["l"])
            cur_candle["close"] = bar["c"]
            cur_candle["volume"] += bar["v"]

        if bar_end == period_end:
            cur_candle = state["current_candle"]
            ohlc = OHLC(
                symbol=state["symbol"],
                timeframe=state["timeframe"],
                timestamp=cur_candle["period_start"],
                open=cur_candle["open"],
                high=cur_candle["high"],
                low=cur_candle["low"],
                close=cur_candle["close"],
                volume=cur_candle["volume"],
            )
            self._candle_queue.put_nowait(ohlc)
            self._logger.debug(f"Completed candle: {ohlc}")
            state["current_candle"] = None

    def _on_daily_bar(self, bar: Bar) -> None:
        """Handle daily bar updates.

        Args:
            bar: Bar object from Alpaca stream
        """
        if not hasattr(self, "_aggregation_state"):
            return

        state = self._aggregation_state

        ohlc = OHLC(
            symbol=state["symbol"],
            timeframe=state["timeframe"],
            timestamp=bar.timestamp,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )
        self._candle_queue.put_nowait(ohlc)
        self._logger.debug(f"Daily candle: {ohlc}")

    def _get_period_start(
        self, timestamp: datetime, timeframe_seconds: int
    ) -> datetime:
        """Calculate the start of the period for a given timestamp.

        Args:
            timestamp: The timestamp to calculate period start for
            timeframe_seconds: The timeframe in seconds

        Returns:
            The start of the period
        """
        epoch_seconds = int(timestamp.timestamp())
        period_start_epoch = (epoch_seconds // timeframe_seconds) * timeframe_seconds
        return datetime.fromtimestamp(period_start_epoch, tz=timestamp.tzinfo)

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
            order_id=str(alpaca_order.id),
            symbol=alpaca_order.symbol,
            quantity=float(alpaca_order.qty) if alpaca_order.qty else None,
            executed_quantity=float(alpaca_order.filled_qty),
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
