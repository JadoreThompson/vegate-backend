from datetime import datetime, date
from typing import List

from engine.ohlcv import OHLCV
from engine.enums import Timeframe
from engine.models import OrderRequest, OrderSide, OrderType, TimeInForce
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext


def create_ohlcv_sequence(
    symbol: str,
    start_time: datetime,
    prices: List[float],
    timeframe: Timeframe = Timeframe.m1,
) -> List[OHLCV]:
    """
    Create a sequence of OHLCV candles from a list of prices.

    Args:
        symbol: Trading symbol
        start_time: Starting timestamp
        prices: List of closing prices
        timeframe: Candle timeframe

    Returns:
        List of OHLCV objects
    """
    candles = []
    tf_seconds = timeframe.get_seconds()

    for i, price in enumerate(prices):
        timestamp = datetime.fromtimestamp(start_time.timestamp() + (i * tf_seconds))

        candle = OHLCV(
            symbol=symbol,
            timestamp=timestamp,
            open=price - 0.5,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0 + (i * 100),
            timeframe=timeframe,
        )
        candles.append(candle)

    return candles


def create_uptrend_ohlcv(
    symbol: str = "AAPL",
    num_candles: int = 10,
    start_price: float = 100.0,
    increment: float = 1.0,
) -> List[OHLCV]:
    """Create OHLCV data with upward price trend"""
    prices = [start_price + (i * increment) for i in range(num_candles)]
    return create_ohlcv_sequence(
        symbol=symbol, start_time=datetime(2024, 1, 1, 9, 30), prices=prices
    )


def create_downtrend_ohlcv(
    symbol: str = "AAPL",
    num_candles: int = 10,
    start_price: float = 100.0,
    decrement: float = 1.0,
) -> List[OHLCV]:
    """Create OHLCV data with downward price trend"""
    prices = [start_price - (i * decrement) for i in range(num_candles)]
    return create_ohlcv_sequence(
        symbol=symbol, start_time=datetime(2024, 1, 1, 9, 30), prices=prices
    )


def create_volatile_ohlcv(symbol: str = "AAPL", num_candles: int = 10) -> List[OHLCV]:
    """Create OHLCV data with high volatility"""
    prices = []
    base_price = 100.0

    for i in range(num_candles):
        if i % 2 == 0:
            prices.append(base_price + (i * 2))
        else:
            prices.append(base_price - (i * 2))

    return create_ohlcv_sequence(
        symbol=symbol, start_time=datetime(2024, 1, 1, 9, 30), prices=prices
    )


def create_flat_ohlcv(
    symbol: str = "AAPL", num_candles: int = 10, price: float = 100.0
) -> List[OHLCV]:
    """Create OHLCV data with flat prices"""
    prices = [price] * num_candles
    candles = []

    for i in range(num_candles):
        candle = OHLCV(
            symbol=symbol,
            timestamp=datetime(2024, 1, 1, 9, 30 + i),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        candles.append(candle)

    return candles


class BuyOnceStrategy(BaseStrategy):
    """Test strategy that buys once on first candle"""

    def __init__(self, symbol: str = "AAPL", quantity: float = 10.0):
        self.symbol = symbol
        self.quantity = quantity
        self.executed = False

    def on_candle(self, context: StrategyContext):
        if not self.executed:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=self.quantity,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.executed = True


class BuyAndHoldStrategy(BaseStrategy):
    """Test strategy that buys and holds"""

    def __init__(self, symbol: str = "AAPL", quantity: float = 10.0):
        self.symbol = symbol
        self.quantity = quantity
        self.has_position = False

    def on_candle(self, context: StrategyContext):
        if not self.has_position:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=self.quantity,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.has_position = True


class BuyAndSellOnceStrategy(BaseStrategy):
    """Test strategy that buys early and sells later"""

    def __init__(
        self,
        symbol: str = "AAPL",
        quantity: float = 10.0,
        buy_candle: int = 1,
        sell_candle: int = 5,
    ):
        self.symbol = symbol
        self.quantity = quantity
        self.buy_candle = buy_candle
        self.sell_candle = sell_candle
        self.candle_count = 0
        self.has_position = False

    def on_candle(self, context: StrategyContext):
        self.candle_count += 1

        if self.candle_count == self.buy_candle and not self.has_position:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=self.quantity,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.has_position = True

        elif self.candle_count == self.sell_candle and self.has_position:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=self.quantity,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.has_position = False


class LimitOrderStrategy(BaseStrategy):
    """Test strategy that uses limit orders"""

    def __init__(
        self, symbol: str = "AAPL", quantity: float = 10.0, limit_price: float = 99.0
    ):
        self.symbol = symbol
        self.quantity = quantity
        self.limit_price = limit_price
        self.executed = False

    def on_candle(self, context: StrategyContext):
        if not self.executed:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=self.quantity,
                limit_price=self.limit_price,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.executed = True


class StopOrderStrategy(BaseStrategy):
    """Test strategy that uses stop orders"""

    def __init__(
        self, symbol: str = "AAPL", quantity: float = 10.0, stop_price: float = 105.0
    ):
        self.symbol = symbol
        self.quantity = quantity
        self.stop_price = stop_price
        self.executed = False

    def on_candle(self, context: StrategyContext):
        if not self.executed:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.STOP,
                quantity=self.quantity,
                stop_price=self.stop_price,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.executed = True


class NoTradeStrategy(BaseStrategy):
    """Test strategy that never trades"""

    def on_candle(self, context: StrategyContext):
        pass


class MultiTradeStrategy(BaseStrategy):
    """Test strategy that trades multiple times"""

    def __init__(self, symbol: str = "AAPL", trades_per_session: int = 3):
        self.symbol = symbol
        self.trades_per_session = trades_per_session
        self.trade_count = 0
        self.candle_count = 0

    def on_candle(self, context: StrategyContext):
        self.candle_count += 1

        if self.trade_count < self.trades_per_session and self.candle_count % 2 == 1:
            order = OrderRequest(
                symbol=self.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1.0,
                time_in_force=TimeInForce.GTC,
            )
            context.broker.submit_order(order)
            self.trade_count += 1
