"""
Tests for strategy context: context object, indicators, and historical data.

This module tests the StrategyContext that strategies use to access market data,
historical information, technical indicators, and execute trades.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
import numpy as np

from src.engine.context.base import StrategyContext, HistoricalData
from src.engine.context.indicators import IndicatorMixin
from src.engine.backtesting.data_loader import OHLCBar, OHLCDataLoader, Timeframe
from src.engine.backtesting.simulated_broker import SimulatedBroker
from src.engine.models import OrderType, OrderSide


# ============================================================================
# HistoricalData Tests
# ============================================================================


class TestHistoricalData:
    """Test the HistoricalData container for OHLCV data."""

    def test_historical_data_initialization(self, base_timestamp):
        """Test creating HistoricalData with raw data."""
        timestamps = [base_timestamp + timedelta(minutes=i) for i in range(5)]
        opens = [100.0, 101.0, 102.0, 103.0, 104.0]
        highs = [101.0, 102.0, 103.0, 104.0, 105.0]
        lows = [99.0, 100.0, 101.0, 102.0, 103.0]
        closes = [100.5, 101.5, 102.5, 103.5, 104.5]
        volumes = [1000, 1100, 1200, 1300, 1400]

        hist = HistoricalData(timestamps, opens, highs, lows, closes, volumes)

        assert len(hist) == 5
        assert hist.timestamps == timestamps
        assert hist.opens == opens
        assert hist.highs == highs
        assert hist.lows == lows
        assert hist.closes == closes
        assert hist.volumes == volumes

    def test_historical_data_from_bars(self, sample_bars):
        """Test creating HistoricalData from OHLC bars."""
        hist = HistoricalData.from_bars(sample_bars)

        assert len(hist) == len(sample_bars)
        assert hist.closes[0] == sample_bars[0].close
        assert hist.opens[-1] == sample_bars[-1].open

    def test_historical_data_array_access(self, sample_bars):
        """Test array-style access to OHLCV data."""
        hist = HistoricalData.from_bars(sample_bars)

        closes = hist["close"]
        assert isinstance(closes, list)
        assert len(closes) == len(sample_bars)

        opens = hist["open"]
        assert isinstance(opens, list)

        highs = hist["high"]
        assert isinstance(highs, list)

        lows = hist["low"]
        assert isinstance(lows, list)

        volumes = hist["volume"]
        assert isinstance(volumes, list)

    def test_historical_data_invalid_key_raises_error(self, sample_bars):
        """Test that invalid key raises KeyError."""
        hist = HistoricalData.from_bars(sample_bars)

        with pytest.raises(KeyError, match="Unknown field"):
            _ = hist["invalid"]

    def test_historical_data_len(self, sample_bars):
        """Test __len__ returns correct number of bars."""
        hist = HistoricalData.from_bars(sample_bars)
        assert len(hist) == len(sample_bars)


# ============================================================================
# StrategyContext Tests
# ============================================================================


class TestStrategyContext:
    """Test the StrategyContext provided to strategies."""

    @pytest.mark.asyncio
    async def test_context_initialization(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test initializing strategy context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        assert context.timestamp == base_timestamp
        assert context.current_bar == bars
        assert context.broker == broker

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_current_bar_access(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test accessing current bar data through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        assert context.bar("AAPL") == sample_bar
        assert context.open("AAPL") == 150.0
        assert context.high("AAPL") == 152.0
        assert context.low("AAPL") == 149.0
        assert context.close("AAPL") == 151.0
        assert context.volume("AAPL") == 1000000

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_missing_symbol_returns_none(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test that missing symbol returns None."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        assert context.bar("TSLA") is None
        assert context.open("TSLA") is None
        assert context.close("TSLA") is None

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_symbols_list(
        self, base_timestamp, multi_symbol_bars, mock_db_session
    ):
        """Test getting list of available symbols."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {bar.symbol: bar for bar in multi_symbol_bars}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        symbols = context.symbols()
        assert len(symbols) == 3
        assert "AAPL" in symbols
        assert "TSLA" in symbols
        assert "MSFT" in symbols

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_buy_order(self, base_timestamp, sample_bar, mock_db_session):
        """Test placing buy order through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        response = await context.buy("AAPL", quantity=10.0)

        assert response is not None
        assert response.symbol == "AAPL"
        assert response.side == OrderSide.BUY

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_sell_order(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test placing sell order through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        # First buy some shares
        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        await context.buy("AAPL", quantity=10.0)

        # Now sell
        response = await context.sell("AAPL", quantity=10.0)

        assert response is not None
        assert response.symbol == "AAPL"
        assert response.side == OrderSide.SELL

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_limit_order(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test placing limit order through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        response = await context.buy(
            "AAPL", quantity=10.0, order_type=OrderType.LIMIT, limit_price=145.0
        )

        assert response is not None
        assert response.order_type == OrderType.LIMIT

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_get_position(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test getting position through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # No position initially
        position = await context.get_position("AAPL")
        assert position is None

        # Buy some shares
        await context.buy("AAPL", quantity=10.0)

        # Should have position now
        position = await context.get_position("AAPL")
        assert position is not None
        assert position.quantity == 10.0

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_get_all_positions(
        self, base_timestamp, multi_symbol_bars, mock_db_session
    ):
        """Test getting all positions through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)

        for bar in multi_symbol_bars:
            broker.set_current_price(bar.symbol, bar.close)

        bars = {bar.symbol: bar for bar in multi_symbol_bars}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Buy multiple symbols
        await context.buy("AAPL", quantity=10.0)
        await context.buy("TSLA", quantity=5.0)

        positions = await context.get_all_positions()
        assert len(positions) == 2

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_close_position(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test closing position through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Buy shares
        await context.buy("AAPL", quantity=10.0)

        # Close position
        response = await context.close_position("AAPL")

        assert response is not None
        assert response.side == OrderSide.SELL

        # Position should be closed
        position = await context.get_position("AAPL")
        assert position is None

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_get_account(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test getting account info through context."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        account = await context.get_account()

        assert account is not None
        assert account.cash == 100000.0
        assert account.portfolio_value == 100000.0

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_history_caching(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test that historical data is cached."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # First call should fetch data
        hist1 = await context.history("AAPL", bars=20)

        # Second call should return cached data
        hist2 = await context.history("AAPL", bars=20)

        assert hist1 is hist2  # Same object reference

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_context_clear_cache(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test clearing historical data cache."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Fetch some data
        await context.history("AAPL", bars=20)

        # Clear cache
        context.clear_cache()

        # Cache should be empty
        assert len(context._cache) == 0

        await broker.disconnect()


# ============================================================================
# Technical Indicators Tests
# ============================================================================


class EnhancedContext(StrategyContext, IndicatorMixin):
    """Context with indicator mixin for testing."""

    pass


class TestTechnicalIndicators:
    """Test technical indicators provided by IndicatorMixin."""

    @pytest.mark.asyncio
    async def test_sma_calculation(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test Simple Moving Average calculation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        sma = await context.sma("AAPL", period=20)

        assert isinstance(sma, float)
        assert sma > 0

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_sma_invalid_period_raises_error(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test that invalid period raises ValueError."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        with pytest.raises(ValueError, match="period must be positive"):
            await context.sma("AAPL", period=0)

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_ema_calculation(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test Exponential Moving Average calculation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        ema = await context.ema("AAPL", period=20)

        assert isinstance(ema, float)
        assert ema > 0

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_rsi_calculation(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test RSI calculation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        rsi = await context.rsi("AAPL", period=14)

        assert isinstance(rsi, float)
        assert 0 <= rsi <= 100

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_macd_calculation(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test MACD calculation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        macd_line, signal_line, histogram = await context.macd("AAPL")

        assert isinstance(macd_line, float)
        assert isinstance(signal_line, float)
        assert isinstance(histogram, float)
        assert histogram == pytest.approx(macd_line - signal_line, rel=0.001)

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_macd_invalid_periods_raises_error(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test that invalid MACD periods raise ValueError."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Fast >= slow should raise error
        with pytest.raises(ValueError, match="fast period must be less than slow"):
            await context.macd("AAPL", fast=26, slow=12)

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_bollinger_bands_calculation(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test Bollinger Bands calculation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        upper, middle, lower = await context.bollinger_bands("AAPL", period=20)

        assert isinstance(upper, float)
        assert isinstance(middle, float)
        assert isinstance(lower, float)
        assert upper > middle > lower

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_bollinger_bands_custom_std_dev(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test Bollinger Bands with custom standard deviation."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        upper1, middle1, lower1 = await context.bollinger_bands("AAPL", std_dev=2.0)
        upper2, middle2, lower2 = await context.bollinger_bands("AAPL", std_dev=3.0)

        # Wider bands with higher std dev
        assert (upper2 - middle2) > (upper1 - middle1)
        assert (middle2 - lower2) > (middle1 - lower1)

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_indicator_caching(
        self, base_timestamp, sample_bars, mock_db_session_with_data
    ):
        """Test that indicators benefit from history caching."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(mock_db_session_with_data)

        context = EnhancedContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Calculate SMA twice - second should use cached data
        sma1 = await context.sma("AAPL", period=20)
        sma2 = await context.sma("AAPL", period=20)

        assert sma1 == sma2

        await broker.disconnect()
