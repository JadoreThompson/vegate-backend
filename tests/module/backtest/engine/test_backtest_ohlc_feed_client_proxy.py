from unittest.mock import MagicMock, PropertyMock
from uuid import uuid4

import pytest

from module.backtest.engine.ohlc_feed_client_proxy import (
    BacktestOHLCFeedClientProxy,
)
from vegate.markets.schema import OHLC as OHLCSchema


def _make_candle(symbol="AAPL"):
    return OHLCSchema(
        open=100.0,
        high=105.0,
        low=99.0,
        close=102.0,
        volume=1000,
        symbol=symbol,
        broker="alpaca",
        market_type="stocks",
        timeframe="1m",
        timestamp=1000000,
    )


@pytest.fixture
def mock_ohlc_client():
    client = MagicMock()
    client.candles.return_value = iter([])
    return client


@pytest.fixture
def mock_runner():
    runner = MagicMock()
    type(runner).is_running = PropertyMock(return_value=True)
    return runner


@pytest.fixture
def proxy(mock_ohlc_client, mock_runner):
    return BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)


class TestBacktestOHLCFeedClientProxy:

    def test_candles_yields_all_when_running(self, mock_ohlc_client, mock_runner):
        candles = [_make_candle("AAPL"), _make_candle("MSFT")]
        mock_ohlc_client.candles.return_value = iter(candles)

        proxy = BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)
        result = list(proxy.candles())

        assert len(result) == 2
        assert result[0].symbol == "AAPL"
        assert result[1].symbol == "MSFT"

    def test_candles_yields_throws_when_not_running(self, mock_ohlc_client, mock_runner):
        type(mock_runner).is_running = PropertyMock(return_value=False)
        mock_ohlc_client.candles.return_value = iter([_make_candle()])

        proxy = BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)
        with pytest.raises(Exception, match="Runner stopped, exiting candle generator"):
            result = list(proxy.candles())

    def test_candles_stops_mid_stream_when_runner_stops(
        self, mock_ohlc_client, mock_runner
    ):
        is_running_mock = PropertyMock(side_effect=[True, True, False])
        type(mock_runner).is_running = is_running_mock

        candles = [_make_candle("AAPL"), _make_candle("MSFT"), _make_candle("GOOG")]
        candles_iter = iter(candles)
        mock_ohlc_client.candles.return_value = candles_iter

        with pytest.raises(Exception, match="Runner stopped, exiting candle generator"):
            proxy = BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)
            result = list(proxy.candles())

        assert is_running_mock.call_count == 3
        
        with pytest.raises(StopIteration):
            next(candles_iter)

    def test_delegates_other_attributes(self, mock_ohlc_client, mock_runner):
        mock_ohlc_client.cur_candle = _make_candle("DELEGATED")

        proxy = BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)

        assert proxy.cur_candle.symbol == "DELEGATED"

    def test_candles_delegation(self, mock_ohlc_client, mock_runner):
        mock_ohlc_client.candles.return_value = iter([_make_candle("DELEGATED")])

        proxy = BacktestOHLCFeedClientProxy(mock_ohlc_client, mock_runner)

        result = list(proxy.candles())
        assert len(result) == 1
        assert result[0].symbol == "DELEGATED"
