import asyncio
import json
import logging
from typing import AsyncGenerator, Optional, List, Generator
from datetime import datetime, date, timedelta
from uuid import UUID

import websockets
from alpaca.common.exceptions import APIError
from alpaca.trading.stream import TradingStream
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    TimeInForce as AlpacaTimeInForce,
    OrderType as AlpacaOrderType,
    OrderStatus as AlpacaOrderStatus,
)
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
)
from sqlalchemy import insert, select, update

from config import BACKEND_DOMAIN, BACKEND_SUB_DOMAIN, BARS_WS_TOKEN, IS_PRODUCTION
from db_models import Orders, StrategyDeployments
from utils.db import get_db_sess_sync
from .base import BaseBroker
from .exc import (
    BrokerError,
    AuthenticationError,
    OrderRejectedError,
    InsufficientFundsError,
    RateLimitError,
    BrokerConnectionError,
)
from .http import HTTPSessMixin
from ..enums import BrokerType, Timeframe
from ..models import (
    OrderRequest,
    OrderResponse,
    Account,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
)
from ..ohlcv import OHLCV


class AlpacaBroker(HTTPSessMixin, BaseBroker):
    def __init__(
        self,
        deplyoment_id: UUID,
        oauth_token: str | None = None,
        api_key: str | None = None,
        secret_key: str | None = None,
        is_crypto: bool = False,
        paper: bool = True,
    ):
        super().__init__()
        self._deployment_id = deplyoment_id
        self._oauth_token = oauth_token
        self._api_key = api_key
        self._secret_key = secret_key
        self._is_crypto = is_crypto
        self._paper = paper
        self._recorded_starting_balance = False
        self._base_url = "https://data.alpaca.markets"
        self._stream_client: TradingStream | None = None
        self._stream_task: asyncio.Task | None = None
        self._logger = logging.getLogger(f"{type(self).__name__}.{self._deployment_id}")

    @property
    def supports_disconnect_async(self) -> bool:
        return True

    def connect(self) -> None:
        try:
            self._apply_rate_limit()

            self._trading_client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                oauth_token=self._oauth_token,
            )

            # Test client
            self._trading_client.get_account()

            loop = asyncio.get_running_loop()
            self._stream_task = loop.create_task(self._listen_trade_updates())

            self._connected = True
            self._logger.info("Connected to Alpaca")

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

    async def disconnect_async(self) -> None:
        """
        Disconnect from Alpaca.

        Alpaca's REST client doesn't maintain persistent connections,
        so this mainly cleans up the client reference.
        """
        self._trading_client = None
        self._connected = False

        if self._stream_task is not None and not self._stream_task.done():
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass

        self._logger.info("Disconnected from Alpaca")

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
        if not self._connected or not self._trading_client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            # Convert our order to Alpaca format
            alpaca_order = self._convert_order_to_alpaca(order)

            # Submit order
            alpaca_response = self._trading_client.submit_order(alpaca_order)

            # Convert response to our format
            response = self._convert_order_from_alpaca(alpaca_response)

            self._logger.debug(
                f"Submitted order: {response.order_id} for {order.symbol}"
            )
            self._on_order_submit(response)
            return response

        except APIError as e:
            self._handle_api_error(e, "submit_order")
        except Exception as e:
            self._log_error("submit_order", e)
            raise BrokerError(f"Failed to submit order: {e}") from e

    def _on_order_submit(self, order_response: OrderResponse):
        if self._recorded_starting_balance:
            return

        self._recorded_starting_balance = True
        account = self.get_account()

        with get_db_sess_sync() as db_sess:
            db_sess.execute(
                update(StrategyDeployments).values(starting_balance=account.cash)
            )
            db_sess.execute(
                insert(Orders).values(
                    symbol=order_response.symbol,
                    side=order_response.side.value,
                    order_type=order_response.order_type.value,
                    quantity=order_response.quantity,
                    filled_quantity=order_response.filled_quantity,
                    limit_price=(order_response.limit_price),
                    stop_price=(order_response.stop_price),
                    avg_fill_price=(order_response.avg_fill_price),
                    status=order_response.status.value,
                    time_in_force=order_response.time_in_force.value,
                    submitted_at=order_response.submitted_at,
                    filled_at=order_response.filled_at,
                    broker_order_id=order_response.order_id,
                    deployment_id=self._deployment_id,
                    broker_metadata=order_response.broker_metadata,
                )
            )

            db_sess.commit()

    def _on_order_update(self, payload: dict):
        # NOTE: Only supports fill events. Also assumes
        # this payload is a fill event.
        order = payload["data"]["order"]
        if payload["data"]["event"] != "fill":
            return

        with get_db_sess_sync() as db_sess:
            db_order = db_sess.scalar(
                select(Orders).where(Orders.broker_order_id == order["id"])
            )
            if order is None:
                self._logger.debug("Failed to find order object")
                return

            db_order.filled_quantity = float(order["filled_qty"])
            db_order.avg_fill_price = float(order["filled_avg_price"])
            db_order.filled_at = datetime.fromisoformat(order["filled_at"])
            db_order.broker_metadata = payload["data"]
            db_order.status = self._convert_status_from_alpaca(order["status"])
            db_sess.commit()

        self._logger.debug("Successfully updated order object")

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
        if not self._connected or not self._trading_client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            self._trading_client.cancel_order_by_id(order_id)
            self._logger.info(f"Cancelled order: {order_id}")
            return True

        except APIError as e:
            if e.status_code == 404:
                self._logger.warning(f"Order not found: {order_id}")
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
        if not self._connected or not self._trading_client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_order = self._trading_client.get_order_by_id(order_id)
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
        if not self._connected or not self._trading_client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            request = GetOrdersRequest(
                status="open", symbols=[symbol] if symbol else None
            )

            alpaca_orders = self._trading_client.get_orders(filter=request)

            return [self._convert_order_from_alpaca(order) for order in alpaca_orders]

        except APIError as e:
            self._handle_api_error(e, "get_open_orders")
        except Exception as e:
            self._log_error("get_open_orders", e)
            raise BrokerError(f"Failed to get open orders: {e}") from e

    def get_account(self) -> Account:
        """
        Get account information.

        Returns:
            Account details

        Raises:
            BrokerError: If account data cannot be retrieved
        """
        if not self._connected or not self._trading_client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_account = self._trading_client.get_account()

            return Account(
                account_id=str(alpaca_account.id),
                equity=float(alpaca_account.equity),
                cash=alpaca_account.cash,
            )

        except APIError as e:
            self._handle_api_error(e, "get_account")
        except Exception as e:
            self._log_error("get_account", e)
            raise BrokerError(f"Failed to get account: {e}") from e

    def get_historic_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        prev_bars: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[OHLCV]:
        """
        Get historical OHLCV data from Alpaca.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for bars (1m, 5m, 15m, 30m, 1h, 1d)
            prev_bars: Number of bars to fetch (if start_date not provided)
            start_date: Start date for historical data
            end_date: End date for historical data (defaults to today)

        Returns:
            List of OHLCV objects
        """

        return list(
            self.yield_historic_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )
        )

    def yield_historic_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        prev_bars: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Generator[OHLCV, None, None]:
        """
        Generator that yields historical OHLCV data from Alpaca.

        This method fetches data in chunks to avoid memory issues with large datasets.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for bars
            prev_bars: Number of bars to fetch (if start_date not provided)
            start_date: Start date for historical data
            end_date: End date for historical data

        Yields:
            OHLCV objects one at a time
        """

        def _convert_tf(tf: Timeframe):
            tf_map = {
                Timeframe.m1: "1Min",
                Timeframe.m5: "5Min",
                Timeframe.m15: "15Min",
                Timeframe.m30: "30Min",
                Timeframe.H1: "1Hour",
                Timeframe.H4: "4Hour",
                Timeframe.D1: "1Day",
                Timeframe.W1: "1Week",
                Timeframe.M1: "1Month",
                Timeframe.Y1: "12Month",
            }

            return tf_map[tf]

        if start_date is None and prev_bars is not None:
            end_date = end_date or date.today()
            days_back = self._estimate_days_for_bars(prev_bars, timeframe)
            start_date = end_date - timedelta(days=days_back)
        elif start_date is None:
            raise BrokerError("Either prev_bars or start_date must be provided")

        end_date = end_date or date.today()
        fmt_start = start_date.isoformat()
        fmt_end = end_date.isoformat()
        page_count = 0

        if self._oauth_token:
            headers = {"Authorization": f"Bearer {self._oauth_token}"}
        else:
            headers = {
                "APCA-API-KEY-ID": self._api_key,
                "APCA-API-SECRET-KEY": self._secret_key,
            }

        result = []
        next_page_token = None
        while True:
            page_count += 1
            params = (
                {"start": fmt_start, "end": fmt_end}
                if not next_page_token
                else {"page_token": next_page_token}
            )

            if self._is_crypto:
                endpoint = f"{self._base_url}/v1beta3/crypto/us/bars"
                params["symbols"] = [symbol]
            else:
                endpoint = (
                    f"{self._base_url}/v2/stocks/bars?"
                    "feed=iex&"
                    f"symbols={symbol}&"
                    f"timeframe={_convert_tf(timeframe)}"
                )

            self._logger.debug(f"Fetching page {page_count} for {symbol}")
            rsp = self._http_sess.get(endpoint, headers=headers, params=params)
            if not rsp.ok:
                print(rsp.text)
            rsp.raise_for_status()
            data = rsp.json()            

            candles = data.get("bars", {}).get(symbol)

            if not candles:
                self._logger.debug(f"No more bars available for {symbol}")
                break

            self._logger.debug(f"Retrieved {len(candles)} bars on page {page_count}")

            for c in candles:
                yield OHLCV(
                    symbol=symbol,
                    timestamp=datetime.fromisoformat(c["t"]),
                    timeframe=timeframe,
                    open=c["o"],
                    high=c["h"],
                    low=c["l"],
                    close=c["c"],
                    volume=c['v']
                )

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                self._logger.debug(
                    f"Completed fetching historical data for {symbol} - {page_count} pages processed"
                )
                break

        return result

    async def yield_ohlcv_async(
        self, symbol: str, timeframe: Timeframe
    ) -> AsyncGenerator[OHLCV, None]:
        """
        Generator that yields real-time OHLCV data from Alpaca.

        This method continuously yields the latest bar data. For real-time streaming,
        you would typically use Alpaca's WebSocket API. This implementation polls
        for the latest bar periodically.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for bars

        Yields:
            OHLCV objects as they become available
        """

        scheme = "wss" if IS_PRODUCTION else "ws"
        market = "crypto" if self._is_crypto else "stocks"

        async with websockets.connect(
            f"{scheme}://{BACKEND_SUB_DOMAIN}{BACKEND_DOMAIN}/markets/{market}/bars"
        ) as ws:
            await ws.send(json.dumps({"token": BARS_WS_TOKEN}), text=True)

            msg = await ws.recv()

            payload = json.loads(msg)

            if not (
                payload.get("type") == "message"
                and payload.get("message") == "connected"
            ):
                raise BrokerConnectionError("Failed to connect to market bars server")

            await ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "broker": BrokerType.ALPACA,
                        "symbols": [[symbol, timeframe]],
                    }
                )
            )

            while True:
                msg = await ws.recv()
                payload = json.loads(msg)
                yield OHLCV(
                    symbol=payload["symbol"],
                    timestamp=datetime.fromisoformat(payload["timestamp"]),
                    open=payload["open"],
                    high=payload["high"],
                    low=payload["low"],
                    close=payload["close"],
                    volume=payload["volume"],
                    timeframe=payload["timeframe"],
                )

    async def _listen_trade_updates(self):
        """
        Connects to Alpaca's trade update WebSocket using websockets,
        authenticates with OAuth, subscribes to trade updates, and logs incoming updates.
        """

        self._logger.debug(f"Connecting to Alpaca trade stream")

        try:
            async with websockets.connect(
                "wss://paper-api.alpaca.markets/stream",
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                auth_msg = {
                    "action": "authenticate",
                    "data": {"oauth_token": self._oauth_token},
                }

                await ws.send(json.dumps(auth_msg).encode())
                self._logger.debug("Sent OAuth authentication message")

                msg = await ws.recv()
                data = json.loads(msg)
                if data["data"]["status"] == "unauthorized":
                    raise Exception(
                        f"Authentication failed connecting to trading stream. Received payload {data}"
                    )

                sub_msg = {"action": "listen", "data": {"streams": ["trade_updates"]}}
                await ws.send(json.dumps(sub_msg))
                self._logger.debug("Subscribed to trade_updates")

                while True:
                    try:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        self._logger.debug("Received trade update")

                        if "order" in data["data"]:
                            self._on_order_update(data)

                    except websockets.exceptions.ConnectionClosed as e:
                        self._logger.error(f"WebSocket closed: {e}")
                        break

                    except Exception as e:
                        self._logger.exception(f"Error receiving message: {e}")
                        break
        except Exception as e:
            self._logger.error(f"Error handling trade stream {e}")

    @staticmethod
    def _convert_status_from_alpaca(status: str):
        status_map = {"filled": OrderStatus.FILLED}

        return status_map[status]

    @staticmethod
    def _convert_tf_to_alpaca(tf: TimeInForce) -> AlpacaTimeInForce:
        tif_map = {
            TimeInForce.DAY: AlpacaTimeInForce.DAY,
            TimeInForce.GTC: AlpacaTimeInForce.GTC,
            TimeInForce.IOC: AlpacaTimeInForce.IOC,
            TimeInForce.FOK: AlpacaTimeInForce.FOK,
        }
        return tif_map[tf]

    @staticmethod
    def _convert_tf_from_alpaca(tf: AlpacaTimeInForce) -> TimeInForce:
        tif_map = {
            AlpacaTimeInForce.DAY: TimeInForce.DAY,
            AlpacaTimeInForce.GTC: TimeInForce.GTC,
            AlpacaTimeInForce.IOC: TimeInForce.IOC,
            AlpacaTimeInForce.FOK: TimeInForce.FOK,
        }
        return tif_map[tf]

    def _convert_order_to_alpaca(self, order: OrderRequest):
        """Convert our OrderRequest to Alpaca order request."""
        # Convert side
        side = (
            AlpacaOrderSide.BUY if order.side == OrderSide.BUY else AlpacaOrderSide.SELL
        )

        time_in_force = self._convert_tf_to_alpaca(order.time_in_force)

        # Create appropriate order type
        if order.order_type == OrderType.MARKET:
            return MarketOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                client_order_id=order.client_order_id,
            )
        elif order.order_type == OrderType.LIMIT:
            return LimitOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                limit_price=order.limit_price,
                client_order_id=order.client_order_id,
            )
        elif order.order_type == OrderType.STOP:
            return StopOrderRequest(
                symbol=order.symbol,
                qty=order.quantity,
                side=side,
                time_in_force=time_in_force,
                stop_price=order.stop_price,
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
                client_order_id=order.client_order_id,
            )
        else:
            raise BrokerError(f"Unsupported order type: {order.order_type}")

    def _convert_order_from_alpaca(self, order: AlpacaOrder) -> OrderResponse:
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
        status = status_map.get(order.status, OrderStatus.PENDING)

        # Convert side
        side = OrderSide.BUY if order.side == AlpacaOrderSide.BUY else OrderSide.SELL

        # Convert order type
        type_map = {
            AlpacaOrderType.MARKET: OrderType.MARKET,
            AlpacaOrderType.LIMIT: OrderType.LIMIT,
            AlpacaOrderType.STOP: OrderType.STOP,
            AlpacaOrderType.STOP_LIMIT: OrderType.STOP_LIMIT,
            AlpacaOrderType.TRAILING_STOP: OrderType.TRAILING_STOP,
        }
        order_type = type_map.get(order.type, OrderType.MARKET)

        return OrderResponse(
            order_id=str(order.id),
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=side,
            order_type=order_type,
            quantity=float(order.qty),
            filled_quantity=float(order.filled_qty or 0),
            status=status,
            submitted_at=order.submitted_at,
            filled_at=order.filled_at,
            avg_fill_price=(
                float(order.filled_avg_price) if order.filled_avg_price else None
            ),
            limit_price=order.limit_price,
            stop_price=order.stop_price,
            time_in_force=self._convert_tf_from_alpaca(order.time_in_force),
            broker_metadata={
                "alpaca_order_class": (
                    str(order.order_class) if hasattr(order, "order_class") else None
                ),
                "alpaca_time_in_force": str(order.time_in_force),
            },
        )

    def _estimate_days_for_bars(self, num_bars: int, timeframe: Timeframe) -> int:
        """Estimate number of days needed to get the requested number of bars."""
        # Account for market hours and non-trading days
        seconds_per_bar = timeframe.get_seconds()

        if timeframe == Timeframe.D1:
            # Daily bars: account for weekends (roughly 5 trading days per 7 calendar days)
            return int(num_bars * 1.4) + 7
        else:
            # Intraday bars: account for trading hours (6.5 hours per day) and weekends
            trading_seconds_per_day = 6.5 * 3600
            bars_per_day = trading_seconds_per_day / seconds_per_bar
            days_needed = num_bars / bars_per_day
            # Add buffer for weekends
            return int(days_needed * 1.4) + 7

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
