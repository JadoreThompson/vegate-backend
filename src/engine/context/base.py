import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from ..models import (
    OrderRequest,
    OrderResponse,
    OrderType,
    OrderSide,
    Position,
    Account,
)
from ..brokers.base import BaseBroker
from ..backtesting.data_loader import OHLCBar, OHLCDataLoader, Timeframe

logger = logging.getLogger(__name__)


class HistoricalData:
    """
    Container for historical OHLCV data.

    Provides array-style access to historical prices and volumes,
    making it easy to perform calculations on historical data.

    Attributes:
        timestamps: List of bar timestamps
        opens: List of opening prices
        highs: List of high prices
        lows: List of low prices
        closes: List of closing prices
        volumes: List of volumes
    """

    def __init__(
        self,
        timestamps: List[datetime],
        opens: List[float],
        highs: List[float],
        lows: List[float],
        closes: List[float],
        volumes: List[int],
    ):
        """
        Initialize historical data container.

        Args:
            timestamps: List of bar timestamps
            opens: List of opening prices
            highs: List of high prices
            lows: List of low prices
            closes: List of closing prices
            volumes: List of volumes
        """
        self.timestamps = timestamps
        self.opens = opens
        self.highs = highs
        self.lows = lows
        self.closes = closes
        self.volumes = volumes

    @classmethod
    def from_bars(cls, bars: List[OHLCBar]) -> "HistoricalData":
        """
        Create HistoricalData from list of OHLC bars.

        Args:
            bars: List of OHLCBar objects

        Returns:
            HistoricalData instance populated with bar data
        """
        return cls(
            timestamps=[b.timestamp for b in bars],
            opens=[b.open for b in bars],
            highs=[b.high for b in bars],
            lows=[b.low for b in bars],
            closes=[b.close for b in bars],
            volumes=[b.volume for b in bars],
        )

    def __len__(self) -> int:
        """Return number of bars in historical data."""
        return len(self.timestamps)

    def __getitem__(self, key: str) -> List[float]:
        """
        Array-style access to OHLCV data.

        Args:
            key: Data field ('open', 'high', 'low', 'close', 'volume')

        Returns:
            List of values for the requested field

        Raises:
            KeyError: If key is not a valid field name

        Example:
            closes = hist_data['close']
            highs = hist_data['high']
        """
        if key == "open":
            return self.opens
        elif key == "high":
            return self.highs
        elif key == "low":
            return self.lows
        elif key == "close":
            return self.closes
        elif key == "volume":
            return self.volumes
        else:
            raise KeyError(f"Unknown field: {key}")


class StrategyContext:
    """
    Runtime context provided to trading strategies.

    This class provides strategies with a unified interface to access market data,
    historical information, and execute trades. It works seamlessly in both
    live trading and backtesting environments.

    The context is immutable per bar - it reflects a single point in time during
    strategy execution. Historical data is fetched on-demand and cached for
    performance.

    Attributes:
        timestamp: Current bar timestamp
        broker: Broker instance for trade execution
        current_bar: Current OHLC bar data for all symbols

    Example:
        def my_strategy(ctx: StrategyContext):
            # Get current price
            price = ctx.close('AAPL')

            # Get historical data
            history = ctx.history('AAPL', bars=20)
            avg_price = sum(history['close']) / len(history['close'])

            # Check position
            position = ctx.get_position('AAPL')

            # Place order if conditions met
            if price > avg_price and not position:
                ctx.buy('AAPL', quantity=10)
    """

    def __init__(
        self,
        timestamp: datetime,
        bars: Dict[str, OHLCBar],
        broker: BaseBroker,
        data_loader: OHLCDataLoader,
        timeframe: Timeframe = Timeframe.M1,
    ):
        """
        Initialize strategy context.

        Args:
            timestamp: Current bar timestamp
            bars: Dictionary mapping symbols to current OHLC bars
            broker: Broker instance for trade execution
            data_loader: Data loader for historical data access
            timeframe: Default timeframe for historical data queries
        """
        self._timestamp = timestamp
        self._bars = bars
        self._broker = broker
        self._data_loader = data_loader
        self._timeframe = timeframe
        self._cache: Dict[str, Any] = {}
        logger.debug(f"StrategyContext initialized at {timestamp}")

    # Time Access Properties

    @property
    def timestamp(self) -> datetime:
        """
        Get current bar timestamp.

        Returns:
            Current timestamp
        """
        return self._timestamp

    @property
    def current_bar(self) -> Dict[str, OHLCBar]:
        """
        Get dictionary of current bars for all symbols.

        Returns:
            Dictionary mapping symbol to OHLCBar
        """
        return self._bars

    @property
    def broker(self) -> BaseBroker:
        """
        Get broker instance.

        Returns:
            Broker instance for direct access if needed
        """
        return self._broker

    # Current Bar Data Access

    def bar(self, symbol: str) -> Optional[OHLCBar]:
        """
        Get current OHLC bar for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            OHLCBar if available, None otherwise
        """
        return self._bars.get(symbol)

    def open(self, symbol: str) -> Optional[float]:
        """
        Get current open price for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Open price if available, None otherwise
        """
        bar = self._bars.get(symbol)
        return bar.open if bar else None

    def high(self, symbol: str) -> Optional[float]:
        """
        Get current high price for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            High price if available, None otherwise
        """
        bar = self._bars.get(symbol)
        return bar.high if bar else None

    def low(self, symbol: str) -> Optional[float]:
        """
        Get current low price for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Low price if available, None otherwise
        """
        bar = self._bars.get(symbol)
        return bar.low if bar else None

    def close(self, symbol: str) -> Optional[float]:
        """
        Get current close price for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Close price if available, None otherwise
        """
        bar = self._bars.get(symbol)
        return bar.close if bar else None

    def volume(self, symbol: str) -> Optional[int]:
        """
        Get current volume for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Volume if available, None otherwise
        """
        bar = self._bars.get(symbol)
        return bar.volume if bar else None

    # Historical Data Access

    def history(
        self,
        symbol: str,
        bars: int = 100,
        timeframe: Optional[Timeframe] = None,
    ) -> HistoricalData:
        """
        Get historical OHLCV data for a symbol.

        This method fetches historical bars and caches them for performance.
        Subsequent calls with the same parameters return cached data.

        Args:
            symbol: Trading symbol
            bars: Number of historical bars to fetch (default: 100)
            timeframe: Bar timeframe (defaults to context's timeframe)

        Returns:
            HistoricalData object with OHLCV arrays

        Raises:
            ValueError: If bars <= 0
            Exception: If data loading fails

        Example:
            history = ctx.history('AAPL', bars=50)
            recent_closes = history['close']
            avg_close = sum(recent_closes) / len(recent_closes)
        """
        if bars <= 0:
            raise ValueError("bars must be positive")

        # Use provided timeframe or fall back to context default
        tf = timeframe or self._timeframe

        # Create cache key
        cache_key = (symbol, bars, tf)

        # Return cached data if available
        if cache_key in self._cache:
            logger.debug(f"Returning cached history for {cache_key}")
            return self._cache[cache_key]

        logger.debug(f"Fetching history for {symbol}: {bars} bars at {tf.value}")

        try:
            # Calculate date range with buffer for weekends/holidays
            end_date = self._timestamp
            days_needed = bars * 2  # Rough approximation with buffer
            start_date = end_date - timedelta(days=days_needed)

            # Fetch data from loader
            all_bars = []
            for batch in self._data_loader.load_data(
                symbols=[symbol],
                start_date=start_date,
                end_date=end_date,
                timeframe=tf,
            ):
                all_bars.extend(batch)

            # Take last N bars
            recent_bars = all_bars[-bars:] if len(all_bars) > bars else all_bars

            if not recent_bars:
                logger.warning(f"No historical data found for {symbol}")
                # Return empty HistoricalData
                hist_data = HistoricalData([], [], [], [], [], [])
            else:
                # Convert to HistoricalData
                hist_data = HistoricalData.from_bars(recent_bars)
                logger.debug(
                    f"Loaded {len(hist_data)} bars for {symbol} " f"(requested {bars})"
                )

            # Cache for future use
            self._cache[cache_key] = hist_data

            return hist_data

        except Exception as e:
            logger.error(f"Error fetching history for {symbol}: {e}", exc_info=True)
            raise

    # Trading Operations (delegate to broker)

    def buy(
        self,
        symbol: str,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        **kwargs,
    ) -> OrderResponse:
        """
        Place a buy order.

        Args:
            symbol: Trading symbol
            quantity: Number of shares to buy
            order_type: Type of order (default: MARKET)
            **kwargs: Additional order parameters (limit_price, stop_price, etc.)

        Returns:
            OrderResponse with order details

        Raises:
            OrderRejectedError: If order is rejected
            InsufficientFundsError: If insufficient funds
            BrokerError: For other errors

        Example:
            # Market order
            order = ctx.buy('AAPL', quantity=10)

            # Limit order
            order = ctx.buy('AAPL', quantity=10,
                                 order_type=OrderType.LIMIT,
                                 limit_price=150.00)
        """
        try:
            order = OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=order_type,
                quantity=quantity,
                limit_price=kwargs.get("limit_price"),
                stop_price=kwargs.get("stop_price"),
                time_in_force=kwargs.get("time_in_force"),
                extended_hours=kwargs.get("extended_hours", False),
                client_order_id=kwargs.get("client_order_id"),
            )
            logger.info(f"Submitting BUY order: {symbol} x{quantity} @ {order_type}")
            return self._broker.submit_order(order)
        except Exception as e:
            logger.error(f"Error placing buy order for {symbol}: {e}", exc_info=True)
            raise

    def sell(
        self,
        symbol: str,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        **kwargs,
    ) -> OrderResponse:
        """
        Place a sell order.

        Args:
            symbol: Trading symbol
            quantity: Number of shares to sell
            order_type: Type of order (default: MARKET)
            **kwargs: Additional order parameters (limit_price, stop_price, etc.)

        Returns:
            OrderResponse with order details

        Raises:
            OrderRejectedError: If order is rejected
            BrokerError: For other errors

        Example:
            # Market order
            order = ctx.sell('AAPL', quantity=10)

            # Limit order
            order = ctx.sell('AAPL', quantity=10,
                                  order_type=OrderType.LIMIT,
                                  limit_price=155.00)
        """
        try:
            order = OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=order_type,
                quantity=quantity,
                limit_price=kwargs.get("limit_price"),
                stop_price=kwargs.get("stop_price"),
                time_in_force=kwargs.get("time_in_force"),
                extended_hours=kwargs.get("extended_hours", False),
                client_order_id=kwargs.get("client_order_id"),
            )
            logger.info(f"Submitting SELL order: {symbol} x{quantity} @ {order_type}")
            return self._broker.submit_order(order)
        except Exception as e:
            logger.error(f"Error placing sell order for {symbol}: {e}", exc_info=True)
            raise

    def close_position(self, symbol: str) -> Optional[OrderResponse]:
        """
        Close entire position for a symbol.

        This is a convenience method that closes the complete position
        with a market order.

        Args:
            symbol: Trading symbol

        Returns:
            OrderResponse if position existed and was closed, None otherwise

        Raises:
            BrokerError: If position cannot be closed

        Example:
            result = ctx.close_position('AAPL')
            if result:
                print(f"Position closed: {result.order_id}")
        """
        try:
            logger.info(f"Closing position for {symbol}")
            return self._broker.close_position(symbol)
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {e}", exc_info=True)
            raise

    # Position & Account Access

    def get_position(self, symbol: str) -> Optional[Position]:
        """
        Get current position for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position if exists, None otherwise

        Raises:
            BrokerError: If position data cannot be retrieved

        Example:
            position = ctx.get_position('AAPL')
            if position:
                print(f"Holding {position.quantity} shares at ${position.average_entry_price}")
        """
        try:
            return self._broker.get_position(symbol)
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {e}", exc_info=True)
            raise

    def get_all_positions(self) -> List[Position]:
        """
        Get all current positions.

        Returns:
            List of all positions

        Raises:
            BrokerError: If position data cannot be retrieved

        Example:
            positions = ctx.get_all_positions()
            for pos in positions:
                print(f"{pos.symbol}: {pos.quantity} shares")
        """
        try:
            return self._broker.get_all_positions()
        except Exception as e:
            logger.error(f"Error getting all positions: {e}", exc_info=True)
            raise

    def get_account(self) -> Account:
        """
        Get account information.

        Returns:
            Account information with balances and buying power

        Raises:
            BrokerError: If account data cannot be retrieved

        Example:
            account = ctx.get_account()
            print(f"Cash: ${account.cash}")
            print(f"Portfolio Value: ${account.portfolio_value}")
        """
        try:
            return self._broker.get_account()
        except Exception as e:
            logger.error(f"Error getting account info: {e}", exc_info=True)
            raise

    # Utility Methods

    def symbols(self) -> List[str]:
        """
        Get list of symbols in current context.

        Returns:
            List of symbol strings

        Example:
            available_symbols = ctx.symbols()
            for symbol in available_symbols:
                price = ctx.close(symbol)
        """
        return list(self._bars.keys())

    def clear_cache(self) -> None:
        """
        Clear the historical data cache.

        This can be useful for memory management in long-running strategies
        or when you want to force a refresh of historical data.

        Example:
            # Periodically clear cache to free memory
            if ctx.timestamp.hour == 0 and ctx.timestamp.minute == 0:
                ctx.clear_cache()
        """
        cache_size = len(self._cache)
        self._cache.clear()
        logger.debug(f"Cleared cache: {cache_size} entries removed")
