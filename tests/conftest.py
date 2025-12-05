"""
Shared pytest fixtures for trading strategy framework tests.

This module provides common fixtures used across all test modules including
mock data, broker configurations, and database session mocks.
"""

import pytest
from datetime import datetime, timedelta
from typing import List, AsyncIterator
from unittest.mock import AsyncMock, MagicMock, Mock
from sqlalchemy.ext.asyncio import AsyncSession

from src.engine.models import (
    OrderRequest,
    OrderResponse,
    Position,
    Account,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
)
from src.engine.backtesting.data_loader import OHLCBar, Timeframe
from src.engine.brokers.rate_limiter import TokenBucketRateLimiter


# === Time and Date Fixtures ===


@pytest.fixture
def base_timestamp():
    """Base timestamp for testing (Jan 1, 2024 09:30:00)."""
    return datetime(2024, 1, 1, 9, 30, 0)


@pytest.fixture
def date_range(base_timestamp):
    """Date range for backtesting (Jan 1-31, 2024)."""
    return {
        "start": base_timestamp,
        "end": base_timestamp + timedelta(days=30),
    }


# === OHLC Bar Fixtures ===


@pytest.fixture
def sample_bar(base_timestamp):
    """
    Create a single OHLC bar for testing.

    Returns:
        OHLCBar with sample data for AAPL
    """
    return OHLCBar(
        symbol="AAPL",
        timestamp=base_timestamp,
        open=150.0,
        high=152.0,
        low=149.0,
        close=151.0,
        volume=1000000,
        timeframe=Timeframe.M1,
    )


@pytest.fixture
def sample_bars(base_timestamp):
    """
    Create a sequence of OHLC bars for testing.

    Returns:
        List of 100 OHLCBar objects with incrementing prices
    """
    bars = []
    for i in range(100):
        timestamp = base_timestamp + timedelta(minutes=i)
        base_price = 150.0 + i * 0.1
        bars.append(
            OHLCBar(
                symbol="AAPL",
                timestamp=timestamp,
                open=base_price,
                high=base_price + 1.0,
                low=base_price - 1.0,
                close=base_price + 0.5,
                volume=1000000 + i * 1000,
                timeframe=Timeframe.M1,
            )
        )
    return bars


@pytest.fixture
def multi_symbol_bars(base_timestamp):
    """
    Create bars for multiple symbols at the same timestamp.

    Returns:
        List of OHLCBar objects for AAPL, TSLA, MSFT
    """
    symbols = ["AAPL", "TSLA", "MSFT"]
    base_prices = {"AAPL": 150.0, "TSLA": 200.0, "MSFT": 300.0}

    bars = []
    for symbol in symbols:
        base_price = base_prices[symbol]
        bars.append(
            OHLCBar(
                symbol=symbol,
                timestamp=base_timestamp,
                open=base_price,
                high=base_price + 2.0,
                low=base_price - 2.0,
                close=base_price + 1.0,
                volume=1000000,
                timeframe=Timeframe.M1,
            )
        )
    return bars


# === Order Fixtures ===


@pytest.fixture
def market_order_request():
    """
    Create a sample market order request.

    Returns:
        OrderRequest for buying 100 shares of AAPL
    """
    return OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
        time_in_force=TimeInForce.DAY,
    )


@pytest.fixture
def limit_order_request():
    """
    Create a sample limit order request.

    Returns:
        OrderRequest for buying 100 shares of AAPL at $150
    """
    return OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=100.0,
        limit_price=150.0,
        time_in_force=TimeInForce.GTC,
    )


@pytest.fixture
def filled_order_response(base_timestamp):
    """
    Create a sample filled order response.

    Returns:
        OrderResponse with filled status
    """
    return OrderResponse(
        order_id="ORDER123",
        client_order_id="CLIENT123",
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
        filled_quantity=100.0,
        status=OrderStatus.FILLED,
        submitted_at=base_timestamp,
        filled_at=base_timestamp + timedelta(seconds=1),
        avg_fill_price=150.5,
        broker_metadata={"commission": 1.0, "slippage": 0.15},
    )


# === Position Fixtures ===


@pytest.fixture
def sample_position():
    """
    Create a sample position.

    Returns:
        Position holding 100 shares of AAPL
    """
    return Position(
        symbol="AAPL",
        quantity=100.0,
        average_entry_price=150.0,
        current_price=151.0,
        market_value=15100.0,
        unrealized_pnl=100.0,
        unrealized_pnl_percent=0.67,
        cost_basis=15000.0,
        side=OrderSide.BUY,
    )


@pytest.fixture
def multiple_positions():
    """
    Create multiple positions for testing.

    Returns:
        List of Position objects for AAPL, TSLA, MSFT
    """
    return [
        Position(
            symbol="AAPL",
            quantity=100.0,
            average_entry_price=150.0,
            current_price=151.0,
            market_value=15100.0,
            unrealized_pnl=100.0,
            unrealized_pnl_percent=0.67,
            cost_basis=15000.0,
            side=OrderSide.BUY,
        ),
        Position(
            symbol="TSLA",
            quantity=50.0,
            average_entry_price=200.0,
            current_price=205.0,
            market_value=10250.0,
            unrealized_pnl=250.0,
            unrealized_pnl_percent=2.5,
            cost_basis=10000.0,
            side=OrderSide.BUY,
        ),
        Position(
            symbol="MSFT",
            quantity=75.0,
            average_entry_price=300.0,
            current_price=298.0,
            market_value=22350.0,
            unrealized_pnl=-150.0,
            unrealized_pnl_percent=-0.67,
            cost_basis=22500.0,
            side=OrderSide.BUY,
        ),
    ]


# === Account Fixtures ===


@pytest.fixture
def sample_account(base_timestamp):
    """
    Create a sample account.

    Returns:
        Account with $100,000 initial capital
    """
    return Account(
        account_id="ACC123",
        equity=100000.0,
        available_cash=50000.0,
        buying_power=50000.0,
        portfolio_value=100000.0,
        last_updated=base_timestamp,
    )


# === Broker Configuration Fixtures ===


@pytest.fixture
def broker_config():
    """
    Broker configuration for testing.

    Returns:
        Dictionary with broker credentials and settings
    """
    return {
        "api_key": "test_api_key",
        "secret_key": "test_secret_key",
        "paper": True,
    }


@pytest.fixture
def rate_limiter():
    """
    Create a rate limiter for testing.

    Returns:
        TokenBucketRateLimiter configured for testing
    """
    return TokenBucketRateLimiter(rate=100, per_seconds=60)


# === Database Mock Fixtures ===


@pytest.fixture
def mock_db_session():
    """
    Create a mock database session for testing.

    Returns:
        Mock AsyncSession that can be used in tests
    """
    session = AsyncMock(spec=AsyncSession)

    # Mock execute to return a mock result
    mock_result = MagicMock()
    mock_result.fetchall = MagicMock(return_value=[])
    mock_result.scalar = MagicMock(return_value=0)
    session.execute = AsyncMock(return_value=mock_result)

    return session


@pytest.fixture
def mock_db_session_with_data(sample_bars):
    """
    Create a mock database session that returns sample data.

    Args:
        sample_bars: Sample OHLC bars to return from queries

    Returns:
        Mock AsyncSession configured to return sample data
    """
    session = AsyncMock(spec=AsyncSession)

    # Convert bars to row tuples
    rows = [
        (
            bar.symbol,
            bar.timestamp,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
        )
        for bar in sample_bars
    ]

    mock_result = MagicMock()
    mock_result.fetchall = MagicMock(return_value=rows)
    mock_result.scalar = MagicMock(return_value=len(rows))
    session.execute = AsyncMock(return_value=mock_result)

    return session


# === Equity Curve Fixtures ===


@pytest.fixture
def sample_equity_curve(base_timestamp):
    """
    Create a sample equity curve for metrics testing.

    Returns:
        List of (timestamp, equity) tuples representing portfolio growth
    """
    equity_curve = []
    base_equity = 100000.0

    for i in range(100):
        timestamp = base_timestamp + timedelta(days=i)
        # Simulate some volatility with trend
        equity = base_equity + i * 50 + (i % 10 - 5) * 100
        equity_curve.append((timestamp, equity))

    return equity_curve


@pytest.fixture
def volatile_equity_curve(base_timestamp):
    """
    Create a volatile equity curve with drawdowns for testing.

    Returns:
        List of (timestamp, equity) tuples with significant volatility
    """
    import math

    equity_curve = []
    base_equity = 100000.0

    for i in range(100):
        timestamp = base_timestamp + timedelta(days=i)
        # Simulate volatility with sine wave
        volatility = math.sin(i / 10) * 5000
        trend = i * 30
        equity = base_equity + trend + volatility
        equity_curve.append((timestamp, equity))

    return equity_curve


# === Strategy Function Fixtures ===


@pytest.fixture
def simple_buy_strategy():
    """
    Create a simple buy-and-hold strategy for testing.

    Returns:
        Async function that buys on first bar
    """
    bought = {"value": False}

    async def strategy(context):
        """Simple strategy that buys 10 shares of AAPL once."""
        if not bought["value"]:
            position = await context.get_position("AAPL")
            if not position:
                await context.buy("AAPL", quantity=10)
                bought["value"] = True

    return strategy


@pytest.fixture
def sma_crossover_strategy():
    """
    Create an SMA crossover strategy for testing.

    Returns:
        Async function implementing SMA crossover logic
    """

    async def strategy(context):
        """Strategy that trades based on SMA crossover."""
        symbol = "AAPL"

        # Get current position
        position = await context.get_position(symbol)

        # Get historical data for SMA calculation
        try:
            hist = await context.history(symbol, bars=20)
            if len(hist) < 20:
                return

            # Calculate simple moving averages
            closes = hist["close"]
            sma_short = sum(closes[-5:]) / 5
            sma_long = sum(closes[-20:]) / 20

            current_price = context.close(symbol)

            # Trading logic
            if not position and sma_short > sma_long:
                # Buy signal
                await context.buy(symbol, quantity=10)
            elif position and sma_short < sma_long:
                # Sell signal
                await context.close_position(symbol)
        except Exception:
            # Skip if insufficient data
            pass

    return strategy


# === Mock Broker Fixtures ===


@pytest.fixture
def mock_broker():
    """
    Create a mock broker for testing without actual broker connection.

    Returns:
        Mock broker with common methods configured
    """
    broker = AsyncMock()
    broker.connect = AsyncMock()
    broker.disconnect = AsyncMock()
    broker.submit_order = AsyncMock()
    broker.cancel_order = AsyncMock(return_value=True)
    broker.get_order = AsyncMock()
    broker.get_open_orders = AsyncMock(return_value=[])
    broker.get_position = AsyncMock(return_value=None)
    broker.get_all_positions = AsyncMock(return_value=[])
    broker.close_position = AsyncMock()
    broker.get_account = AsyncMock()

    return broker


# === Pytest Configuration ===


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "requires_db: mark test as requiring database")
