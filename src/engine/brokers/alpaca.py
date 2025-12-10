import asyncio
import json
import logging
import threading
from typing import AsyncGenerator, Optional, List, Generator
from datetime import datetime, date, timedelta
from decimal import Decimal

import websockets
from alpaca.common.exceptions import APIError
from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    TimeInForce as AlpacaTimeInForce,
    OrderType as AlpacaOrderType,
    OrderStatus as AlpacaOrderStatus,
)
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopOrderRequest,
    StopLimitOrderRequest,
    GetOrdersRequest,
)

from config import BACKEND_DOMAIN, BACKEND_SUB_DOMAIN, BARS_WS_TOKEN, IS_PRODUCTION

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
from ..models import (
    OrderRequest,
    OrderResponse,
    Account,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
)
from ..enums import BrokerType, Timeframe
from ..ohlcv import OHLCV


logger = logging.getLogger(__name__)


class AlpacaBroker(HTTPSessMixin, BaseBroker):
    def __init__(
        self,
        oauth_token: str | None = None,
        api_key: str | None = None,
        secret_key: str | None = None,
        is_crypto: bool = False,
        paper: bool = True,
    ):
        super().__init__()
        self._oauth_token = oauth_token
        self._api_key = api_key
        self._secret_key = secret_key
        self._is_crypto = is_crypto
        self._paper = paper
        self._base_url = "https://data.alpaca.markets"

        self._loop: asyncio.AbstractEventLoop | None = None
        self._stream_client: CryptoDataStream | StockDataStream = None
        self._ev: threading.Event = threading.Event()
        self._candle: OHLCV | None = None
        self._ws: websockets.ClientConnection | None = None

    def connect(self) -> None:
        try:
            self._apply_rate_limit()

            self._client = TradingClient(
                api_key=self._api_key,
                secret_key=self._secret_key,
                oauth_token=self._oauth_token,
            )
            # Test client
            self._client.get_account()

            if self._is_crypto:
                stream_cls = CryptoDataStream
            else:
                stream_cls = StockDataStream

            websocket_params = None
            if self._oauth_token is not None:
                websocket_params = {"Authorization": f"Bearer {self._oauth_token}"}
            self._stream_client = stream_cls(
                api_key=self._api_key,
                secret_key=self._secret_key,
                websocket_params=websocket_params,
                raw_data=True,
            )

            self._connected = True
            logger.info("Connected to Alpaca")

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
        self._client = None
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
        if not self._connected or not self._client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            # Convert our order to Alpaca format
            alpaca_order = self._convert_order_to_alpaca(order)

            # Submit order
            alpaca_response = self._client.submit_order(alpaca_order)

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
        if not self._connected or not self._client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            self._client.cancel_order_by_id(order_id)
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
        if not self._connected or not self._client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_order = self._client.get_order_by_id(order_id)
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
        if not self._connected or not self._client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()

            request = GetOrdersRequest(
                status="open", symbols=[symbol] if symbol else None
            )

            alpaca_orders = self._client.get_orders(filter=request)

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
        if not self._connected or not self._client:
            raise BrokerError("Broker not connected")

        try:
            self._apply_rate_limit()
            alpaca_account = self._client.get_account()

            return Account(
                account_id=alpaca_account.id,
                equity=float(alpaca_account.equity),
                available_cash=float(alpaca_account.cash),
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
            avg_fill_price=(
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

        result = []
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
                endpoint = f"{self._base_url}/v2/stocks/{symbol}/bars"

            logger.debug(f"Fetching page {page_count} for {symbol}")
            rsp = self._http_sess.get(endpoint, params=params)
            rsp.raise_for_status()
            data = rsp.json()

            if self._is_crypto:
                candles = data.get("bars", {}).get(symbol)
            else:
                candles = data.get("bars", [])

            if not candles:
                logger.info(f"No more bars available for {symbol}")
                break

            logger.debug(f"Retrieved {len(candles)} bars on page {page_count}")

            for d in candles:
                yield OHLCV(
                    symbol=symbol,
                    timeframe=datetime.fromtimestamp(d["t"]),
                    open=d["o"],
                    high=d["h"],
                    low=d["l"],
                    close=d["c"],
                )

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                logger.info(
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

            to_decimal = lambda v: Decimal(str(v))

            while True:
                msg = await ws.recv()
                payload = json.loads(msg)
                yield OHLCV(
                    symbol=payload["symbol"],
                    timestamp=datetime.fromisoformat(payload["timestamp"]),
                    open=to_decimal(payload["open"]),
                    high=to_decimal(payload["high"]),
                    low=to_decimal(payload["low"]),
                    close=to_decimal(payload["close"]),
                    volume=to_decimal(payload["volume"]),
                    timeframe=payload["timeframe"],
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
