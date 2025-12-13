import os
import pytest
from datetime import datetime, date
from dotenv import load_dotenv

from engine.enums import Timeframe
from engine.models import OrderSide, OrderType, TimeInForce
from engine.ohlcv import OHLCV


@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """Load test environment variables from .env.test"""
    load_dotenv(".env.test", override=True)


@pytest.fixture
def sample_ohlcv_data():
    """Generate sample OHLCV data for testing"""
    base_time = datetime(2024, 1, 1, 9, 30)
    data = []

    prices = [100.0, 101.0, 102.5, 101.5, 103.0, 102.0, 104.0, 103.5, 105.0, 104.5]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price - 0.5,
            high=price + 1.0,
            low=price - 1.0,
            close=price,
            volume=1000.0 + i * 100,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data


@pytest.fixture
def volatile_ohlcv_data():
    """Generate volatile OHLCV data for drawdown testing"""
    base_time = datetime(2024, 1, 1, 9, 30)
    data = []

    prices = [100.0, 110.0, 105.0, 95.0, 90.0, 100.0, 115.0, 110.0, 105.0, 100.0]

    for i, price in enumerate(prices):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=price - 1.0,
            high=price + 2.0,
            low=price - 2.0,
            close=price,
            volume=2000.0 + i * 200,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data


@pytest.fixture
def flat_ohlcv_data():
    """Generate flat OHLCV data (no price movement)"""
    base_time = datetime(2024, 1, 1, 9, 30)
    data = []

    for i in range(10):
        ohlcv = OHLCV(
            symbol="AAPL",
            timestamp=datetime(
                base_time.year,
                base_time.month,
                base_time.day,
                base_time.hour,
                base_time.minute + i,
            ),
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1000.0,
            timeframe=Timeframe.m1,
        )
        data.append(ohlcv)

    return data
