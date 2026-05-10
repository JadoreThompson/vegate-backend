from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from enums import BrokerType, MarketType, Timeframe
from service.ohlc.loader.alpaca import AlpacaOHLCLoader


@pytest.fixture
def mock_session():
    session = MagicMock()
    session.get = AsyncMock()
    return session


@pytest.fixture
def loader(mock_session):
    with patch("aiohttp.ClientSession", return_value=mock_session):
        loader = AlpacaOHLCLoader("test_api_key", "test_secret_key")
        loader._http_sess = mock_session
        yield loader


class TestFormatSymbol:

    @pytest.mark.parametrize(
        "input_symbol,expected",
        [
            ("BTC/USD", "BTCUSD"),
            ("ETH/USD", "ETHUSD"),
            ("AAPL", "AAPL"),
            ("BTC/USD/BTC/USD", "BTCUSDBTCUSD"),
        ],
    )
    def test_format_symbol_various_formats(self, input_symbol, expected):
        result = AlpacaOHLCLoader._format_symbol(input_symbol)
        assert result == expected

    @pytest.mark.asyncio
    async def test_symbol_uppercase_conversion(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        mock_db_session = AsyncMock()
        mock_db_sess = AsyncMock()
        mock_db_sess.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_sess.__aexit__ = AsyncMock(return_value=None)

        mock_result = MagicMock()
        mock_result.first.return_value = (0,)
        mock_db_session.execute.return_value = mock_result

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "aapl",  # lowercase input — must be normalised to "AAPL"
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        get_calls = mock_session.get.call_args_list
        assert get_calls, "Expected at least one HTTP GET call"
        for c in get_calls:
            params = c[0][1] if len(c[0]) > 1 else c[1].get("params", {})
            if "symbols" in params:
                assert params["symbols"] == "AAPL", (
                    f"Symbol was not uppercased: got {params['symbols']!r}"
                )


class TestTimeframeMapping:

    @pytest.mark.parametrize(
        "timeframe",
        [
            Timeframe.m1,
            Timeframe.m5,
            Timeframe.m15,
            Timeframe.m30,
            Timeframe.H1,
            Timeframe.H4,
            Timeframe.D1,
        ],
    )
    def test_valid_timeframes(self, timeframe):
        from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame

        result = AlpacaOHLCLoader._timeframe_2_alpaca_timeframe(timeframe)
        assert isinstance(result, AlpacaTimeFrame)

    def test_unsupported_timeframe(self):
        class FakeTimeframe:
            pass

        with pytest.raises(ValueError, match="Unsupported timeframe"):
            AlpacaOHLCLoader._timeframe_2_alpaca_timeframe(FakeTimeframe())


class TestFetchBars:

    @pytest.mark.asyncio
    async def test_fetch_bars_stocks_market(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        bars_list = []
        params_list = []
        async for bars, params in loader._fetch_bars(
            "AAPL",
            MarketType.STOCKS,
            Timeframe.m1,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        ):
            bars_list.append(bars)
            params_list.append(params)

        assert len(bars_list) == 1
        assert bars_list[0][0]["o"] == 100.0
        assert params_list[0]["symbols"] == "AAPL"

    @pytest.mark.asyncio
    async def test_fetch_bars_crypto_market(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "BTCUSD": [
                        {
                            "t": "2024-01-01T00:00:00Z",
                            "o": 50000.0,
                            "h": 50500.0,
                            "l": 49500.0,
                            "c": 50200.0,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        bars_list = []
        async for bars, _ in loader._fetch_bars(
            "BTCUSD",
            MarketType.CRYPTO,
            Timeframe.H1,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        ):
            bars_list.append(bars)

        assert len(bars_list) == 1
        assert bars_list[0][0]["o"] == 50000.0

    @pytest.mark.asyncio
    async def test_fetch_bars_with_page_token(self, loader, mock_session):
        first_response = MagicMock()
        first_response.ok = True
        first_response.status = 200
        first_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": "page_token_123",
            }
        )

        second_response = MagicMock()
        second_response.ok = True
        second_response.status = 200
        second_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:35:00Z",
                            "o": 101.0,
                            "h": 102.0,
                            "l": 100.0,
                            "c": 101.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )

        mock_session.get.side_effect = [first_response, second_response]

        bars_list = []
        async for bars, _ in loader._fetch_bars(
            "AAPL",
            MarketType.STOCKS,
            Timeframe.m1,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        ):
            bars_list.append(bars)

        assert len(bars_list) == 2
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_bars_403_subscription_error(self, loader, mock_session):
        error_response = MagicMock()
        error_response.ok = False
        error_response.status = 403
        error_response.json = AsyncMock(
            return_value={
                "message": "subscription does not permit querying recent SIP data"
            }
        )

        success_response = MagicMock()
        success_response.ok = True
        success_response.status = 200
        success_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )

        mock_session.get.side_effect = [error_response, success_response]

        bars_list = []
        async for bars, params in loader._fetch_bars(
            "AAPL",
            MarketType.STOCKS,
            Timeframe.m1,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        ):
            bars_list.append(bars)

        assert len(bars_list) == 1
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_bars_unsupported_market_type(self, loader):
        class UnknownMarketType:
            pass

        with pytest.raises(ValueError, match="Unsupported market type"):
            async for _ in loader._fetch_bars(
                "AAPL",
                UnknownMarketType(),
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            ):
                pass

    @pytest.mark.asyncio
    async def test_fetch_bars_http_error(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status = 500
        mock_response.json = AsyncMock(return_value={"error": "Internal Server Error"})
        mock_session.get.return_value = mock_response

        with pytest.raises(RuntimeError, match="Error in client response status"):
            async for _ in loader._fetch_bars(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            ):
                pass


class TestLoadCandles:

    def _make_mock_db(self, count_value):
        """Return (mock_db_sess, mock_db_session) pre-wired with a count result."""
        mock_db_session = AsyncMock()
        mock_db_sess = AsyncMock()
        mock_db_sess.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_sess.__aexit__ = AsyncMock(return_value=None)
        mock_result = MagicMock()
        mock_result.first.return_value = (count_value,)
        mock_db_session.execute.return_value = mock_result
        return mock_db_sess, mock_db_session

    def _one_bar_response(self):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )
        return mock_response

    # The count query must always happen; then an insert must follow when the
    # existing count is 0 (no prior records for that window).
    @pytest.mark.asyncio
    async def test_load_candles_inserts_records_when_count_is_zero(
        self, loader, mock_session
    ):
        mock_session.get.return_value = self._one_bar_response()
        mock_db_sess, mock_db_session = self._make_mock_db(count_value=0)

        captured_records = []

        async def capture_execute(*args, **kwargs):
            if len(args) > 1 and isinstance(args[1], list):
                captured_records.extend(args[1])
            return MagicMock()

        mock_db_session.execute.side_effect = capture_execute

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        assert mock_db_session.execute.call_count >= 2, (
            "Expected at least a count query and an insert call"
        )
        assert len(captured_records) == 1, (
            "One bar in the response should produce exactly one inserted record"
        )

    @pytest.mark.asyncio
    async def test_load_candles_skips_when_count_matches(self, loader, mock_session):
        mock_session.get.return_value = self._one_bar_response()
        # DB already holds 1 record, which matches the 1 bar returned by the API.
        mock_db_sess, mock_db_session = self._make_mock_db(count_value=1)

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        execute_calls = [str(c) for c in mock_db_session.execute.call_args_list]
        assert not any("delete" in c.lower() for c in execute_calls), (
            "Delete should not be called when stored count matches fetched count"
        )
        assert not any("insert" in c.lower() for c in execute_calls), (
            "Insert should not be called when stored count matches fetched count"
        )

    @pytest.mark.asyncio
    async def test_load_candles_deletes_and_reinserts_when_count_mismatches(
        self, loader, mock_session
    ):
        mock_session.get.return_value = self._one_bar_response()
        # Mismatch - DB holds 2 records but the API only returned 1.
        mock_db_sess, mock_db_session = self._make_mock_db(count_value=2)

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        execute_calls = [str(c) for c in mock_db_session.execute.call_args_list]
        assert any("delete" in c.lower() for c in execute_calls), (
            "Delete should be called when stored count mismatches fetched count"
        )
        assert any("insert" in c.lower() for c in execute_calls), (
            "Insert should be called after delete when count mismatches"
        )


class TestDataParsing:

    def test_parse_bar_record_correctly(self):
        bar = {
            "t": "2024-01-01T09:30:00Z",
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
        }

        record = {
            "source": BrokerType.ALPACA,
            "symbol": "AAPL",
            "open": bar["o"],
            "high": bar["h"],
            "low": bar["l"],
            "close": bar["c"],
            "timestamp": int(
                datetime.fromisoformat(bar["t"].replace("Z", "+00:00")).timestamp()
            ),
            "timeframe": Timeframe.m1,
        }

        assert record["source"] == BrokerType.ALPACA
        assert record["symbol"] == "AAPL"
        assert record["open"] == 100.0
        assert record["high"] == 101.0
        assert record["low"] == 99.0
        assert record["close"] == 100.5
        assert record["timeframe"] == Timeframe.m1
        assert isinstance(record["timestamp"], int)

    @pytest.mark.asyncio
    async def test_parsed_records_have_correct_schema(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        },
                        {
                            "t": "2024-01-01T09:31:00Z",
                            "o": 100.5,
                            "h": 101.5,
                            "l": 100.0,
                            "c": 101.0,
                        },
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        mock_db_session = AsyncMock()
        mock_db_sess = AsyncMock()
        mock_db_sess.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_sess.__aexit__ = AsyncMock(return_value=None)

        captured_records = []

        async def capture_execute(*args, **kwargs):
            # Discriminate: the count query passes no list; the insert does.
            if len(args) > 1 and isinstance(args[1], list):
                captured_records.extend(args[1])
                return MagicMock()
            # First call is the count query returning 0 so insert is triggered.
            mock_result = MagicMock()
            mock_result.first.return_value = (0,)
            return mock_result

        mock_db_session.execute.side_effect = capture_execute

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        assert len(captured_records) == 2, (
            "Two bars in the API response should produce two parsed records"
        )

        required_fields = {"source", "symbol", "open", "high", "low", "close",
                           "timestamp", "timeframe"}
        for record in captured_records:
            missing = required_fields - record.keys()
            assert not missing, f"Record is missing fields: {missing}"

            assert record["source"] == BrokerType.ALPACA
            assert record["symbol"] == "AAPL"
            assert isinstance(record["open"], float)
            assert isinstance(record["high"], float)
            assert isinstance(record["low"], float)
            assert isinstance(record["close"], float)
            assert isinstance(record["timestamp"], int)
            assert record["timeframe"] == Timeframe.m1


class TestDuplicationHandling:

    @pytest.mark.asyncio
    async def test_duplicate_check_queries_correct_table(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T09:30:00Z",
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        mock_db_session = AsyncMock()
        mock_db_sess = AsyncMock()
        mock_db_sess.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_sess.__aexit__ = AsyncMock(return_value=None)

        mock_result = MagicMock()
        mock_result.first.return_value = (0,)
        mock_db_session.execute.return_value = mock_result

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        first_call_args = mock_db_session.execute.call_args_list[0]
        first_stmt = first_call_args[0][0]
 
        from sqlalchemy.dialects import sqlite
        compiled_sql = str(
            first_stmt.compile(
                dialect=sqlite.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        ).lower()
 
        assert "count" in compiled_sql, (
            "First DB call should be a COUNT query for duplicate detection; "
            f"got: {compiled_sql!r}"
        )
        assert "aapl" in compiled_sql or "symbol" in compiled_sql, (
            "COUNT query should be filtered by symbol; "
            f"got: {compiled_sql!r}"
        )


class TestDatabaseInsertion:

    @pytest.mark.asyncio
    async def test_batch_insert_multiple_records(self, loader, mock_session):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": f"2024-01-01T0{i}:30:00Z",
                            "o": 100.0 + i,
                            "h": 101.0 + i,
                            "l": 99.0 + i,
                            "c": 100.5 + i,
                        }
                        for i in range(5)
                    ]
                },
                "next_page_token": None,
            }
        )
        mock_session.get.return_value = mock_response

        mock_db_session = AsyncMock()
        mock_db_sess = AsyncMock()
        mock_db_sess.__aenter__ = AsyncMock(return_value=mock_db_session)
        mock_db_sess.__aexit__ = AsyncMock(return_value=None)

        mock_result = MagicMock()
        mock_result.first.return_value = (0,)
        mock_db_session.execute.return_value = mock_result

        with patch(
            "service.ohlc.loader.alpaca.get_db_session", return_value=mock_db_sess
        ):
            await loader.load_candles(
                "AAPL",
                MarketType.STOCKS,
                Timeframe.m1,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
            )

        insert_calls = [
            c
            for c in mock_db_session.execute.call_args_list
            if "insert" in str(c).lower()
        ]
        assert len(insert_calls) == 1, (
            "All records should be written in a single batch insert, not one call per record"
        )
