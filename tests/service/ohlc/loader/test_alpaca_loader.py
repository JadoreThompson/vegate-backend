import pytest
import pytest_asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import aiohttp
from sqlalchemy import delete, select, func

from enums import BrokerType, MarketType, Timeframe
from infra.db.model import OHLC
from infra.db.model.instrument import Instrument
from infra.db.utils import get_db_session, get_db_sess_sync
from service.ohlc.loader.alpaca import AlpacaOHLCLoader


@pytest.fixture
def alpaca_loader():
    return AlpacaOHLCLoader(api_key="test-api-key", secret_key="test-secret-key")


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(OHLC))
        db_sess.execute(
            delete(Instrument).where(Instrument.broker_type == BrokerType.ALPACA)
        )
        db_sess.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestFormatSymbol:
    """Unit tests for symbol formatting."""

    def test_format_symbol_removes_slashes(self, alpaca_loader):
        assert alpaca_loader._format_symbol("BTC/USD") == "BTCUSD"

    def test_format_symbol_no_change_without_slashes(self, alpaca_loader):
        assert alpaca_loader._format_symbol("AAPL") == "AAPL"

    def test_format_symbol_empty_string(self, alpaca_loader):
        assert alpaca_loader._format_symbol("") == ""


class TestParseCandle:
    """Unit tests for parsing individual candles."""

    def test_parse_candle_success(self, alpaca_loader):
        instrument_id = uuid4()
        candle = {
            "t": "2024-01-01T10:00:00Z",
            "o": 100.0,
            "h": 105.0,
            "l": 99.0,
            "c": 102.0,
            "v": 1000,
        }

        result = alpaca_loader._parse_candle(candle, Timeframe.H1, instrument_id)

        assert result["open"] == 100.0
        assert result["high"] == 105.0
        assert result["low"] == 99.0
        assert result["close"] == 102.0
        assert result["volume"] == 1000
        assert result["timeframe"] == Timeframe.H1
        assert result["instrument_id"] == str(instrument_id)
        assert isinstance(result["timestamp"], int)

    def test_parse_candle_with_different_timeframe(self, alpaca_loader):
        instrument_id = uuid4()
        candle = {
            "t": "2024-06-15T14:30:00+00:00",
            "o": 50.0,
            "h": 55.0,
            "l": 48.0,
            "c": 52.0,
            "v": 5000,
        }

        result = alpaca_loader._parse_candle(candle, Timeframe.m5, instrument_id)

        assert result["timeframe"] == Timeframe.m5
        assert result["timestamp"] == int(
            datetime(2024, 6, 15, 14, 30, tzinfo=UTC).timestamp()
        )


class TestFetchBars:
    """Unit tests for fetching bars from Alpaca API."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_bars_missing_params_raises(self, alpaca_loader):
        with pytest.raises(ValueError, match="Either params or all of"):
            async for _ in alpaca_loader._fetch_bars(MarketType.STOCKS):
                pass

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_bars_with_params_only(self, alpaca_loader):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T10:00:00Z",
                            "o": 100,
                            "h": 105,
                            "l": 99,
                            "c": 102,
                            "v": 1000,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        alpaca_loader._http_sess = mock_session

        params = {
            "symbols": "AAPL",
            "start": "2024-01-01",
            "end": "2024-01-02",
            "timeframe": "1Hour",
        }

        bars_list = []
        async for bars, returned_params in alpaca_loader._fetch_bars(
            MarketType.STOCKS, params=params
        ):
            bars_list.append(bars)

        assert len(bars_list) == 1
        mock_session.get.assert_called_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_bars_pagination(self, alpaca_loader):
        mock_response1 = MagicMock()
        mock_response1.ok = True
        mock_response1.status = 200
        mock_response1.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T10:00:00Z",
                            "o": 100,
                            "h": 105,
                            "l": 99,
                            "c": 102,
                            "v": 1000,
                        }
                    ]
                },
                "next_page_token": "token123",
            }
        )

        mock_response2 = MagicMock()
        mock_response2.ok = True
        mock_response2.status = 200
        mock_response2.json = AsyncMock(
            return_value={
                "bars": {
                    "AAPL": [
                        {
                            "t": "2024-01-01T11:00:00Z",
                            "o": 102,
                            "h": 108,
                            "l": 101,
                            "c": 107,
                            "v": 2000,
                        }
                    ]
                },
                "next_page_token": None,
            }
        )

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=[mock_response1, mock_response2])
        alpaca_loader._http_sess = mock_session

        params = {
            "symbols": "AAPL",
            "start": "2024-01-01",
            "end": "2024-01-02",
            "timeframe": "1Hour",
        }

        bars_list = []
        async for bars, returned_params in alpaca_loader._fetch_bars(
            MarketType.STOCKS, params=params
        ):
            bars_list.append(bars)

        assert len(bars_list) == 2
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_bars_http_error_raises(self, alpaca_loader):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status = 500
        mock_response.json = AsyncMock(
            return_value={"message": "Internal Server Error"}
        )

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        alpaca_loader._http_sess = mock_session

        params = {
            "symbols": "AAPL",
            "start": "2024-01-01",
            "end": "2024-01-02",
            "timeframe": "1Hour",
        }

        with pytest.raises(RuntimeError, match="Error in client response status: 500"):
            async for _ in alpaca_loader._fetch_bars(MarketType.STOCKS, params=params):
                pass

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_bars_403_sip_error_breaks(self, alpaca_loader):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status = 403
        mock_response.json = AsyncMock(
            return_value={
                "message": "subscription does not permit querying recent SIP data"
            }
        )

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        alpaca_loader._http_sess = mock_session

        params = {
            "symbols": "AAPL",
            "start": "2024-01-01",
            "end": "2024-01-02",
            "timeframe": "1Hour",
        }

        bars_list = []
        async for bars, returned_params in alpaca_loader._fetch_bars(
            MarketType.STOCKS, params=params
        ):
            bars_list.append(bars)

        # Should break out of loop on 403 SIP error
        assert len(bars_list) == 0


class TestPersistRecords:
    """Unit tests for persisting records to database."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_records_empty_list_returns_zero(self, alpaca_loader):
        result = await alpaca_loader._persist_records(uuid4(), "AAPL", Timeframe.H1, [])
        assert result == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_records_new_records(self, alpaca_loader, db_sess):
        # First create an instrument
        instrument = Instrument(
            symbol="TEST",
            native_symbol="TEST",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        records = [
            {
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 102.0,
                "timestamp": int(datetime(2024, 1, 1, 10, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 1000,
                "instrument_id": str(instrument.id),
            }
        ]

        count = await alpaca_loader._persist_records(
            instrument.id, "TEST", Timeframe.H1, records
        )

        assert count == 1

        # Verify record exists
        async with get_db_session() as new_sess:
            res = await new_sess.execute(
                select(OHLC).where(OHLC.instrument_id == instrument.id)
            )
            ohlc_records = res.scalars().all()
            assert len(ohlc_records) == 1
            assert ohlc_records[0].open == 100.0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_records_skips_existing(self, alpaca_loader, db_sess):
        # Create instrument
        instrument = Instrument(
            symbol="TEST2",
            native_symbol="TEST2",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        records = [
            {
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 102.0,
                "timestamp": int(datetime(2024, 1, 1, 10, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 1000,
                "instrument_id": str(instrument.id),
            }
        ]

        # First persist
        count1 = await alpaca_loader._persist_records(
            instrument.id, "TEST2", Timeframe.H1, records
        )
        assert count1 == 1

        # Second persist should skip
        count2 = await alpaca_loader._persist_records(
            instrument.id, "TEST2", Timeframe.H1, records
        )
        assert count2 == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_persist_records_overwrites_partial(self, alpaca_loader, db_sess):
        # Create instrument
        instrument = Instrument(
            symbol="TEST3",
            native_symbol="TEST3",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        records1 = [
            {
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 102.0,
                "timestamp": int(datetime(2024, 1, 1, 10, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 1000,
                "instrument_id": str(instrument.id),
            }
        ]

        # First persist
        count1 = await alpaca_loader._persist_records(
            instrument.id, "TEST3", Timeframe.H1, records1
        )
        assert count1 == 1

        # Different records in same time range
        records2 = [
            {
                "open": 200.0,
                "high": 205.0,
                "low": 199.0,
                "close": 202.0,
                "timestamp": int(datetime(2024, 1, 1, 10, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 2000,
                "instrument_id": str(instrument.id),
            },
            {
                "open": 300.0,
                "high": 305.0,
                "low": 299.0,
                "close": 302.0,
                "timestamp": int(datetime(2024, 1, 1, 11, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 3000,
                "instrument_id": str(instrument.id),
            },
        ]

        # Should delete existing and insert new
        count2 = await alpaca_loader._persist_records(
            instrument.id, "TEST3", Timeframe.H1, records2
        )
        assert count2 == 2

        # Verify only 2 records exist
        async with get_db_session() as new_sess:
            res = await new_sess.execute(
                select(func.count(OHLC.ohlc_id)).where(
                    OHLC.instrument_id == instrument.id
                )
            )
            assert res.scalar() == 2


class TestGetOrCreateInstrumentId:
    """Unit tests for instrument ID retrieval/creation."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_get_existing_instrument(self, alpaca_loader, db_sess):
        instrument = Instrument(
            symbol="EXISTING",
            native_symbol="EXISTING",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        instrument_id = await alpaca_loader._get_or_create_instrument_id(
            "EXISTING", MarketType.STOCKS
        )

        assert instrument_id == instrument.id

    @pytest.mark.asyncio(loop_scope="session")
    async def test_create_instrument_formats_symbol(self, alpaca_loader, db_sess):
        instrument_id = await alpaca_loader._get_or_create_instrument_id(
            "BTC/USD", MarketType.CRYPTO
        )

        async with get_db_session() as new_sess:
            result = await new_sess.execute(
                select(Instrument).where(Instrument.native_symbol == "BTC/USD")
            )
            instrument = result.scalar_one()
            assert instrument.symbol == "BTCUSD"


class TestLoadCandles:
    """Unit tests for the main load_candles method."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_load_candles_success(self, alpaca_loader):
        mock_bars = [
            {
                "t": "2024-01-01T10:00:00Z",
                "o": 100,
                "h": 105,
                "l": 99,
                "c": 102,
                "v": 1000,
            },
            {
                "t": "2024-01-01T11:00:00Z",
                "o": 102,
                "h": 108,
                "l": 101,
                "c": 107,
                "v": 2000,
            },
        ]

        mock_fetch = MagicMock()
        mock_fetch.__aiter__.return_value = iter(
            [(mock_bars, {"start": "2024-01-01", "end": "2024-01-02"})]
        )
        alpaca_loader._fetch_bars = MagicMock(return_value=mock_fetch)

        mock_persist = AsyncMock(return_value=2)
        alpaca_loader._persist_records = mock_persist

        mock_instrument_id = uuid4()
        mock_get_instrument = AsyncMock(return_value=mock_instrument_id)
        alpaca_loader._get_or_create_instrument_id = mock_get_instrument

        await alpaca_loader.load_candles(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            timeframe=Timeframe.H1,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )

        mock_get_instrument.assert_awaited_once_with("AAPL", MarketType.STOCKS)
        mock_persist.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_load_candles_multiple_batches(self, alpaca_loader):
        mock_bars1 = [
            {
                "t": "2024-01-01T10:00:00Z",
                "o": 100,
                "h": 105,
                "l": 99,
                "c": 102,
                "v": 1000,
            },
        ]
        mock_bars2 = [
            {
                "t": "2024-01-01T11:00:00Z",
                "o": 102,
                "h": 108,
                "l": 101,
                "c": 107,
                "v": 2000,
            },
        ]

        async def mock_fetch_generator():
            yield mock_bars1, {"start": "2024-01-01", "end": "2024-01-01"}
            yield mock_bars2, {"start": "2024-01-02", "end": "2024-01-02"}

        alpaca_loader._fetch_bars = MagicMock(return_value=mock_fetch_generator())

        mock_persist = AsyncMock(return_value=1)
        alpaca_loader._persist_records = mock_persist

        mock_instrument_id = uuid4()
        alpaca_loader._get_or_create_instrument_id = AsyncMock(
            return_value=mock_instrument_id
        )

        await alpaca_loader.load_candles(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            timeframe=Timeframe.H1,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )

        assert mock_persist.call_count == 2


class TestIntegration:
    """Integration tests for AlpacaOHLCLoader with real database."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_end_to_end_persist_and_retrieve(self, alpaca_loader, db_sess):
        """Test that records are properly persisted and can be retrieved."""
        # Create instrument first
        instrument = Instrument(
            symbol="INTTEST",
            native_symbol="INTTEST",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
        )
        db_sess.add(instrument)
        await db_sess.flush()
        await db_sess.refresh(instrument)
        await db_sess.commit()

        records = [
            {
                "open": 150.0,
                "high": 155.0,
                "low": 148.0,
                "close": 153.0,
                "timestamp": int(datetime(2024, 6, 1, 10, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 5000,
                "instrument_id": str(instrument.id),
            },
            {
                "open": 153.0,
                "high": 158.0,
                "low": 152.0,
                "close": 157.0,
                "timestamp": int(datetime(2024, 6, 1, 11, 0).timestamp()),
                "timeframe": Timeframe.H1,
                "volume": 6000,
                "instrument_id": str(instrument.id),
            },
        ]

        count = await alpaca_loader._persist_records(
            instrument.id, "INTTEST", Timeframe.H1, records
        )

        assert count == 2

        # Verify records
        async with get_db_session() as new_sess:
            res = await new_sess.execute(
                select(OHLC)
                .where(OHLC.instrument_id == instrument.id)
                .order_by(OHLC.timestamp.asc())
            )
            ohlc_records = res.scalars().all()

            assert len(ohlc_records) == 2
            assert ohlc_records[0].open == 150.0
            assert ohlc_records[0].high == 155.0
            assert ohlc_records[0].low == 148.0
            assert ohlc_records[0].close == 153.0
            assert ohlc_records[0].volume == 5000
            assert ohlc_records[1].open == 153.0
            assert ohlc_records[1].close == 157.0
