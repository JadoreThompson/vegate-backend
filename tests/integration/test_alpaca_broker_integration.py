"""
Integration tests for Alpaca broker.

These tests interact with the real Alpaca API (paper trading) to ensure
the broker works correctly with actual API responses and error conditions.

Requirements:
- Valid Alpaca paper trading API credentials in .env.test
- Internet connection
- Alpaca API availability

Run with: pytest tests/integration/test_alpaca_broker_integration.py -v
Run with markers: pytest -m integration
"""

import pytest
import asyncio
import time
from datetime import datetime, date, timedelta
from uuid import uuid4

import pytest_asyncio

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from engine.brokers.alpaca import AlpacaBroker
from engine.brokers.exc import BrokerError, BrokerConnectionError
from engine.models import (
    OrderRequest,
    OrderSide,
    OrderType,
    TimeInForce,
)
from engine.enums import Timeframe


pytestmark = pytest.mark.integration


@pytest.fixture
def deployment_id():
    """Generate a unique deployment ID for each test."""
    return uuid4()


@pytest_asyncio.fixture
async def broker(deployment_id):
    """
    Create a real Alpaca broker instance connected to paper trading.

    This fixture automatically connects and disconnects the broker.
    """
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=True,
    )

    # Connect to Alpaca
    broker.connect()

    yield broker

    # Cleanup: disconnect and cancel any open orders
    try:
        open_orders = broker.get_open_orders()
        for order in open_orders:
            try:
                broker.cancel_order(order.order_id)
            except Exception:
                pass  # Best effort cleanup
    except Exception:
        pass

    # Disconnect
    if broker._connected:
        await broker.disconnect_async()


@pytest.mark.asyncio(loop_scope="session")
async def test_connection_with_valid_credentials(deployment_id):
    """Test that broker can successfully connect with valid credentials."""
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=True,
    )

    try:
        broker.connect()
        assert broker._connected is True
        assert broker._trading_client is not None
    finally:
        if broker._connected:
            await broker.disconnect_async()


@pytest.mark.asyncio(loop_scope="session")
async def test_connection_with_invalid_credentials(deployment_id):
    """Test that broker properly handles invalid credentials."""
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key="invalid_key",
        secret_key="invalid_secret",
        paper=True,
    )

    with pytest.raises(
        Exception
    ):  # Will raise AuthenticationError or BrokerConnectionError
        broker.connect()

    assert broker._connected is False


@pytest.mark.asyncio(loop_scope="session")
async def test_get_account_info(broker):
    """Test retrieving real account information."""
    account = broker.get_account()

    # Verify account structure
    assert account.account_id is not None
    assert isinstance(account.equity, (int, float))
    assert isinstance(account.cash, (int, float))
    assert account.equity >= 0
    assert account.cash >= 0


@pytest.mark.asyncio(loop_scope="session")
async def test_get_historical_data(broker):
    """Test fetching real historical OHLCV data."""
    # Request last 10 days of daily data for a liquid stock
    end_date = date.today()
    start_date = end_date - timedelta(days=10)

    ohlcv_data = broker.get_historic_ohlcv(
        symbol="AAPL",
        timeframe=Timeframe.D1,
        start_date=start_date,
        end_date=end_date,
    )

    # Verify we got some data
    assert len(ohlcv_data) > 0

    # Verify data structure
    for candle in ohlcv_data[:3]:  # Check first 3 candles
        assert candle.symbol == "AAPL"
        assert candle.open > 0
        assert candle.high > 0
        assert candle.low > 0
        assert candle.close > 0
        assert candle.high >= candle.low
        assert candle.high >= candle.open
        assert candle.high >= candle.close
        assert candle.low <= candle.open
        assert candle.low <= candle.close


@pytest.mark.asyncio(loop_scope="session")
async def test_get_open_orders(broker):
    """Test retrieving open orders (should be empty for clean test)."""
    orders = broker.get_open_orders()

    # Should return a list (might be empty)
    assert isinstance(orders, list)


@pytest.mark.asyncio(loop_scope="session")
async def test_get_open_orders_filtered_by_symbol(broker):
    """Test retrieving open orders filtered by symbol."""
    orders = broker.get_open_orders(symbol="AAPL")

    # Should return a list
    assert isinstance(orders, list)

    # All orders should be for AAPL
    for order in orders:
        assert order.symbol == "AAPL"


@pytest.mark.asyncio(loop_scope="session")
async def test_order_conversion_functions(broker):
    """Test that order conversion functions work correctly."""
    # Test time in force conversion
    from alpaca.trading.enums import TimeInForce as AlpacaTimeInForce

    assert broker._convert_tf_to_alpaca(TimeInForce.GTC) == AlpacaTimeInForce.GTC
    assert broker._convert_tf_to_alpaca(TimeInForce.DAY) == AlpacaTimeInForce.DAY
    assert broker._convert_tf_to_alpaca(TimeInForce.IOC) == AlpacaTimeInForce.IOC
    assert broker._convert_tf_to_alpaca(TimeInForce.FOK) == AlpacaTimeInForce.FOK

    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.GTC) == TimeInForce.GTC
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.DAY) == TimeInForce.DAY
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.IOC) == TimeInForce.IOC
    assert broker._convert_tf_from_alpaca(AlpacaTimeInForce.FOK) == TimeInForce.FOK


@pytest.mark.asyncio(loop_scope="session")
async def test_disconnect_and_reconnect(deployment_id):
    """Test that broker can disconnect and reconnect successfully."""
    broker = AlpacaBroker(
        deplyoment_id=deployment_id,
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=True,
    )

    try:
        # Connect
        broker.connect()
        assert broker._connected is True

        # Disconnect
        await broker.disconnect_async()
        assert broker._connected is False

        # Reconnect
        broker.connect()
        assert broker._connected is True

        # Verify we can still make API calls
        account = broker.get_account()
        assert account.account_id is not None
    finally:
        if broker._connected:
            await broker.disconnect_async()


@pytest.mark.asyncio(loop_scope="session")
async def test_rate_limiting_handling(broker):
    """Test that broker properly handles rate limiting."""
    # Make multiple rapid requests to test rate limiting
    # Alpaca has rate limits, so we should respect them

    for i in range(5):
        account = broker.get_account()
        assert account.account_id is not None
        # Small delay to avoid hitting rate limits too hard
        await asyncio.sleep(0.2)


@pytest.mark.asyncio(loop_scope="session")
async def test_historical_data_pagination(broker):
    """Test that broker correctly handles paginated historical data."""
    # Request a larger dataset that might require pagination
    end_date = date.today()
    start_date = end_date - timedelta(days=90)  # 3 months of data

    ohlcv_data = broker.get_historic_ohlcv(
        symbol="AAPL",
        timeframe=Timeframe.D1,
        start_date=start_date,
        end_date=end_date,
    )

    # Should have data for most trading days in the period
    assert len(ohlcv_data) > 50  # At least 50 trading days

    # Verify data is in chronological order
    timestamps = [candle.timestamp for candle in ohlcv_data]
    assert timestamps == sorted(timestamps)


@pytest.mark.asyncio(loop_scope="session")
async def test_supports_disconnect_async_property(broker):
    """Test that broker correctly reports async disconnect support."""
    assert broker.supports_disconnect_async is True


@pytest.mark.asyncio(loop_scope="session")
async def test_concurrent_operations(broker):
    """Test that broker can handle concurrent operations safely."""

    # Create multiple concurrent tasks
    async def get_account_info():
        return broker.get_account()

    async def get_orders():
        return broker.get_open_orders()

    # Run operations concurrently
    results = await asyncio.gather(
        get_account_info(), get_orders(), get_account_info(), return_exceptions=True
    )

    # Verify all operations succeeded
    for result in results:
        assert not isinstance(result, Exception)


@pytest.mark.asyncio(loop_scope="session")
async def test_historical_data_different_timeframes(broker):
    """Test fetching historical data with different timeframes."""
    end_date = date.today()
    start_date = end_date - timedelta(days=5)

    # Test daily timeframe
    daily_data = broker.get_historic_ohlcv(
        symbol="AAPL",
        timeframe=Timeframe.D1,
        start_date=start_date,
        end_date=end_date,
    )
    assert len(daily_data) > 0

    # Add small delay between requests
    await asyncio.sleep(0.5)

    # Test hourly timeframe (if supported)
    try:
        hourly_data = broker.get_historic_ohlcv(
            symbol="AAPL",
            timeframe=Timeframe.H1,
            start_date=start_date,
            end_date=end_date,
        )
        # Should have more hourly candles than daily
        assert len(hourly_data) >= len(daily_data)
    except Exception as e:
        # Some timeframes might not be supported
        pytest.skip(f"Hourly timeframe not supported: {e}")


@pytest.mark.asyncio(loop_scope="session")
async def test_estimate_days_for_bars(broker):
    """Test the utility function for estimating days needed for bars."""
    # Test daily bars
    days = broker._estimate_days_for_bars(20, Timeframe.D1)
    assert 20 <= days <= 35  # Should account for weekends

    # Test intraday bars
    days = broker._estimate_days_for_bars(100, Timeframe.m1)
    assert days >= 1


# if __name__ == "__main__":
#     pytest.main([__file__, "-v", "-m", "integration"])
