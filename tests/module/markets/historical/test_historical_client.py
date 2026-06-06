from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.schema import OHLC as OHLCSchema
from module.markets.historical import HistoricalDataClient

MODULE_PATH = "module.markets.historical.client"


class TestHistoricalDataClientInit:

    def test_init_sets_name(self):
        client = HistoricalDataClient(base_url="http://localhost:8000")
        assert client._name == "HistoricalDataClient"

    def test_init_sets_logger(self):
        client = HistoricalDataClient(base_url="http://localhost:8000")
        assert client._logger is not None
        assert client._logger.name == "HistoricalDataClient"

    def test_fetch_returns_generator(self):
        from types import GeneratorType

        client = HistoricalDataClient(base_url="http://localhost:8000")
        with patch.object(client._client, "get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {
                "data": [],
                "page": 1,
                "size": 0,
                "has_next": False,
            }
            mock_get.return_value = mock_resp
            result = client.fetch(
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.D1,
                start_date=datetime(1970, 1, 1),
                end_date=datetime(2023, 1, 1),
            )
            assert isinstance(result, GeneratorType)


class TestHistoricalDataClientFetch:

    class TestUnitTest:

        def _make_mock_response(self, data: list[dict], has_next: bool = False):
            mock_resp = MagicMock()
            mock_resp.ok = True
            mock_resp.json.return_value = {
                "data": data,
                "page": 1,
                "size": len(data),
                "has_next": has_next,
            }
            return mock_resp

        def test_fetch_yields_ohlc_schema_objects(self):
            mock_resp = self._make_mock_response(
                [
                    {
                        "open": 100.0,
                        "high": 105.0,
                        "low": 99.0,
                        "close": 102.0,
                        "volume": 1000.0,
                        "timestamp": 1500,
                        "timeframe": "1d",
                        "symbol": "AAPL",
                        "broker": "alpaca",
                        "market_type": "stocks",
                    }
                ]
            )

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(
                client._client, "get", return_value=mock_resp
            ) as mock_get:
                candles = list(
                    client.fetch(
                        symbol="AAPL",
                        market_type=MarketType.STOCKS,
                        broker_type=BrokerType.ALPACA,
                        timeframe=Timeframe.D1,
                        start_date=datetime(1970, 1, 1),
                        end_date=datetime(2023, 1, 1),
                    )
                )

            assert len(candles) == 1
            assert isinstance(candles[0], OHLCSchema)
            assert candles[0].open == 100.0
            assert candles[0].high == 105.0
            assert candles[0].low == 99.0
            assert candles[0].close == 102.0
            assert candles[0].volume == 1000.0
            assert candles[0].symbol == "AAPL"
            assert candles[0].broker == BrokerType.ALPACA
            assert candles[0].market_type == MarketType.STOCKS
            assert candles[0].timeframe == Timeframe.D1
            assert candles[0].timestamp == 1500

        def test_fetch_yields_multiple_candles(self):
            rows = [
                {
                    "open": 100.0 + i,
                    "high": 105.0 + i,
                    "low": 99.0 + i,
                    "close": 102.0 + i,
                    "volume": 1000.0,
                    "timestamp": 1000 + i,
                    "timeframe": "1d",
                    "symbol": "AAPL",
                    "broker": "alpaca",
                    "market_type": "stocks",
                }
                for i in range(5)
            ]
            mock_resp = self._make_mock_response(rows)

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(client._client, "get", return_value=mock_resp):
                candles = list(
                    client.fetch(
                        symbol="AAPL",
                        market_type=MarketType.STOCKS,
                        broker_type=BrokerType.ALPACA,
                        timeframe=Timeframe.D1,
                        start_date=datetime(1970, 1, 1),
                        end_date=datetime(2023, 1, 1),
                    )
                )

            assert len(candles) == 5
            for i, candle in enumerate(candles):
                assert candle.open == 100.0 + i
                assert candle.timestamp == 1000 + i

        def test_fetch_no_results_returns_empty(self):
            mock_resp = self._make_mock_response([])

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(client._client, "get", return_value=mock_resp):
                candles = list(
                    client.fetch(
                        symbol="NONEXISTENT",
                        market_type=MarketType.STOCKS,
                        broker_type=BrokerType.ALPACA,
                        timeframe=Timeframe.D1,
                    )
                )

            assert len(candles) == 0

        def test_fetch_handles_pagination(self):
            page1 = [
                {
                    "open": float(i),
                    "high": float(i + 1),
                    "low": float(i - 1),
                    "close": float(i + 0.5),
                    "volume": 1000.0,
                    "timestamp": i,
                    "timeframe": "1d",
                    "symbol": "AAPL",
                    "broker": "alpaca",
                    "market_type": "stocks",
                }
                for i in range(2)
            ]
            page2 = [
                {
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 1000.0,
                    "timestamp": 10,
                    "timeframe": "1d",
                    "symbol": "AAPL",
                    "broker": "alpaca",
                    "market_type": "stocks",
                }
            ]

            mock_resp1 = MagicMock()
            mock_resp1.ok = True
            mock_resp1.json.return_value = {
                "data": page1,
                "page": 1,
                "size": 2,
                "has_next": True,
            }
            mock_resp2 = MagicMock()
            mock_resp2.ok = True
            mock_resp2.json.return_value = {
                "data": page2,
                "page": 2,
                "size": 1,
                "has_next": False,
            }

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(client._client, "get") as mock_get:
                mock_get.side_effect = [mock_resp1, mock_resp2]
                candles = list(
                    client.fetch(
                        symbol="AAPL",
                        market_type=MarketType.STOCKS,
                        broker_type=BrokerType.ALPACA,
                        timeframe=Timeframe.D1,
                        start_date=datetime(1970, 1, 1),
                        end_date=datetime(2023, 1, 1),
                    )
                )

            assert len(candles) == 3
            assert mock_get.call_count == 2

        def test_fetch_raises_on_error(self):
            mock_resp = MagicMock()
            mock_resp.ok = False
            mock_resp.status_code = 500
            mock_resp.json.return_value = {"detail": "Server error"}

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(client._client, "get", return_value=mock_resp):
                with pytest.raises(Exception, match="500 client error"):
                    list(
                        client.fetch(
                            symbol="AAPL",
                            market_type=MarketType.STOCKS,
                            broker_type=BrokerType.ALPACA,
                            timeframe=Timeframe.D1,
                            start_date=datetime(1970, 1, 1),
                            end_date=datetime(2023, 1, 1),
                        )
                    )

        def test_fetch_correct_url_and_params(self):
            mock_resp = self._make_mock_response([])

            client = HistoricalDataClient(base_url="http://localhost:8000")
            with patch.object(
                client._client, "get", return_value=mock_resp
            ) as mock_get:
                list(
                    client.fetch(
                        symbol="AAPL",
                        market_type=MarketType.STOCKS,
                        broker_type=BrokerType.ALPACA,
                        timeframe=Timeframe.m1,
                        start_date=datetime(1970, 1, 1),
                        end_date=datetime(2023, 1, 1),
                    )
                )

            l = mock_get.call_args_list
            mock_get.assert_called_once_with(
                "http://localhost:8000/markets/bars",
                params={
                    "symbol": "AAPL",
                    "market_type": "stocks",
                    "broker_type": "alpaca",
                    "timeframe": "1m",
                    "page": 1,
                    "limit": 200,
                    "start_date": datetime(1970, 1, 1),
                    "end_date": datetime(2023, 1, 1),
                },
            )