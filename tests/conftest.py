from datetime import datetime

import pytest

from src.engine.brokers.backtest import BacktestBroker
from src.engine.enums import Timeframe
from src.engine.ohlcv import OHLCV


@pytest.fixture
def sample_candle() -> OHLCV:
    """Create a sample OHLCV candle with $100 close price."""
    return OHLCV(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 1, 9, 30),
        open=99.0,
        high=101.0,
        low=98.0,
        close=100.0,
        volume=1000,
        timeframe=Timeframe.m1,
    )


@pytest.fixture
def broker_10k() -> BacktestBroker:
    """Create a broker with $10,000 starting balance."""
    return BacktestBroker(starting_balance=10000.0)


@pytest.fixture
def broker_100k() -> BacktestBroker:
    """Create a broker with $100,000 starting balance."""
    return BacktestBroker(starting_balance=100000.0)
