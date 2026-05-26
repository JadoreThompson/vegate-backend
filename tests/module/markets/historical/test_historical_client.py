import pytest
from datetime import datetime, UTC
from unittest.mock import MagicMock, patch
from uuid import uuid4

from sqlalchemy import delete, insert

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC, Instrument
from module.markets.schema import OHLC as OHLCSchema
from module.markets.historical import HistoricalDataClient
from core.db import get_db_sess_sync, get_db_session

MODULE_PATH = "module.markets.historical.client"


class TestHistoricalDataClientInit:

    def test_init_sets_name(self):
        client = HistoricalDataClient()
        assert client._name == "HistoricalDataClient"

    def test_init_sets_logger(self):
        client = HistoricalDataClient()
        assert client._logger is not None
        assert client._logger.name == "HistoricalDataClient"

    def test_fetch_returns_generator(self):
        client = HistoricalDataClient()
        result = client.fetch(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker_type=BrokerType.ALPACA,
            timeframe=Timeframe.D1,
            start_time=1000,
            end_time=2000,
        )
        from types import GeneratorType
        assert isinstance(result, GeneratorType)


class TestHistoricalDataClientFetch:

    class TestUnitTest:

        def test_fetch_yields_ohlc_schema_objects(self):
            mock_instrument = MagicMock()
            mock_instrument.symbol = "AAPL"
            mock_instrument.broker_type = BrokerType.ALPACA
            mock_instrument.market_type = MarketType.STOCKS

            mock_candle = MagicMock()
            mock_candle.open = 100.0
            mock_candle.high = 105.0
            mock_candle.low = 99.0
            mock_candle.close = 102.0
            mock_candle.volume = 1000.0
            mock_candle.timeframe = Timeframe.D1
            mock_candle.timestamp = 1500

            mock_row = MagicMock()
            mock_row.tuple.return_value = (mock_candle, mock_instrument)

            mock_result = MagicMock()
            mock_result.yield_per.return_value = [mock_row]

            mock_db_sess = MagicMock()
            mock_db_sess.execute.return_value = mock_result

            with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
                mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
                mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

                client = HistoricalDataClient()
                candles = list(client.fetch(
                    symbol="AAPL",
                    market_type=MarketType.STOCKS,
                    broker_type=BrokerType.ALPACA,
                    timeframe=Timeframe.D1,
                    start_time=1000,
                    end_time=2000,
                ))

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
            mock_instrument = MagicMock()
            mock_instrument.symbol = "AAPL"
            mock_instrument.broker_type = BrokerType.ALPACA
            mock_instrument.market_type = MarketType.STOCKS

            rows = []
            for i in range(5):
                candle = MagicMock()
                candle.open = 100.0 + i
                candle.high = 105.0 + i
                candle.low = 99.0 + i
                candle.close = 102.0 + i
                candle.volume = 1000.0
                candle.timeframe = Timeframe.D1
                candle.timestamp = 1000 + i

                row = MagicMock()
                row.tuple.return_value = (candle, mock_instrument)
                rows.append(row)

            mock_result = MagicMock()
            mock_result.yield_per.return_value = rows

            mock_db_sess = MagicMock()
            mock_db_sess.execute.return_value = mock_result

            with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
                mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
                mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

                client = HistoricalDataClient()
                candles = list(client.fetch(
                    symbol="AAPL",
                    market_type=MarketType.STOCKS,
                    broker_type=BrokerType.ALPACA,
                    timeframe=Timeframe.D1,
                    start_time=1000,
                    end_time=2000,
                ))

            assert len(candles) == 5
            for i, candle in enumerate(candles):
                assert candle.open == 100.0 + i
                assert candle.timestamp == 1000 + i

        def test_fetch_no_results_returns_empty(self):
            mock_result = MagicMock()
            mock_result.yield_per.return_value = []

            mock_db_sess = MagicMock()
            mock_db_sess.execute.return_value = mock_result

            with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
                mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
                mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

                client = HistoricalDataClient()
                candles = list(client.fetch(
                    symbol="NONEXISTENT",
                    market_type=MarketType.STOCKS,
                    broker_type=BrokerType.ALPACA,
                    timeframe=Timeframe.D1,
                    start_time=1000,
                    end_time=2000,
                ))

            assert len(candles) == 0

        def test_fetch_passes_yield_per(self):
            mock_instrument = MagicMock()
            mock_instrument.symbol = "AAPL"
            mock_instrument.broker_type = BrokerType.ALPACA
            mock_instrument.market_type = MarketType.STOCKS

            mock_candle = MagicMock()
            mock_candle.open = 100.0
            mock_candle.high = 101.0
            mock_candle.low = 99.0
            mock_candle.close = 100.5
            mock_candle.volume = 1000.0
            mock_candle.timeframe = Timeframe.D1
            mock_candle.timestamp = 1500

            mock_row = MagicMock()
            mock_row.tuple.return_value = (mock_candle, mock_instrument)

            mock_result = MagicMock()
            mock_result.yield_per.return_value = [mock_row]

            mock_db_sess = MagicMock()
            mock_db_sess.execute.return_value = mock_result

            with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
                mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
                mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

                client = HistoricalDataClient()
                list(client.fetch(
                    symbol="AAPL",
                    market_type=MarketType.STOCKS,
                    broker_type=BrokerType.ALPACA,
                    timeframe=Timeframe.D1,
                    start_time=1000,
                    end_time=2000,
                ))

            mock_result.yield_per.assert_called_once_with(1000)

        def test_fetch_instrument_not_found_returns_empty(self):
            mock_result = MagicMock()
            mock_result.yield_per.return_value = []

            mock_db_sess = MagicMock()
            mock_db_sess.execute.return_value = mock_result

            with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
                mock_get_db.return_value.__enter__ = MagicMock(return_value=mock_db_sess)
                mock_get_db.return_value.__exit__ = MagicMock(return_value=None)

                client = HistoricalDataClient()
                candles = list(client.fetch(
                    symbol="FAKE_SYMBOL",
                    market_type=MarketType.STOCKS,
                    broker_type=BrokerType.ALPACA,
                    timeframe=Timeframe.D1,
                    start_time=1000,
                    end_time=2000,
                ))

            assert len(candles) == 0

    class TestIntegration:

        def test_fetch_returns_seeded_data(self):
            symbol = "HISTTEST"
            broker = BrokerType.ALPACA
            market_type = MarketType.STOCKS
            timeframe = Timeframe.m1

            with get_db_sess_sync() as db_sess:
                db_sess.execute(
                    delete(Instrument).where(
                        Instrument.symbol == symbol,
                        Instrument.native_symbol == symbol,
                        Instrument.broker_type == broker,
                        Instrument.market_type == market_type,
                    )
                )

                instrument = db_sess.execute(
                    insert(Instrument).values(
                        symbol=symbol,
                        native_symbol=symbol,
                        broker_type=broker,
                        market_type=market_type,
                    ).returning(Instrument)
                ).scalar()

                start_ts = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())
                candles = [
                    OHLC(
                        timeframe=timeframe,
                        instrument_id=instrument.id,
                        open=float(i),
                        high=float(i + 1),
                        low=float(i - 1),
                        close=float(i + 0.5),
                        volume=1000.0,
                        timestamp=start_ts + i * 60,
                    )
                    for i in range(10)
                ]
                db_sess.add_all(candles)
                db_sess.commit()

            client = HistoricalDataClient()
            result = list(client.fetch(
                symbol=symbol,
                market_type=market_type,
                broker_type=broker,
                timeframe=timeframe,
                start_time=start_ts,
                end_time=start_ts + 600,
            ))

            assert len(result) == 10
            for i, candle in enumerate(result):
                assert isinstance(candle, OHLCSchema)
                assert candle.symbol == symbol
                assert candle.broker == broker
                assert candle.market_type == market_type
                assert candle.timeframe == timeframe
                assert candle.open == float(i)
                assert candle.timestamp == start_ts + i * 60

            with get_db_sess_sync() as db_sess:
                db_sess.execute(delete(OHLC).where(OHLC.instrument_id == instrument.id))
                db_sess.execute(delete(Instrument).where(Instrument.id == instrument.id))
                db_sess.commit()

        def test_fetch_with_time_range_filters(self):            
            symbol = "HISTTS"
            broker = BrokerType.ALPACA
            market_type = MarketType.STOCKS
            timeframe = Timeframe.m1

            with get_db_sess_sync() as db_sess:
                db_sess.execute(
                    delete(Instrument).where(
                        Instrument.symbol == symbol,
                        Instrument.native_symbol == symbol,
                        Instrument.broker_type == broker,
                        Instrument.market_type == market_type,
                    )
                )

                instrument = db_sess.execute(
                    insert(Instrument).values(
                        symbol=symbol,
                        native_symbol=symbol,
                        broker_type=broker,
                        market_type=market_type,
                    ).returning(Instrument)
                ).scalar()

                base = int(datetime(2024, 2, 1, tzinfo=UTC).timestamp())
                candles = [
                    OHLC(
                        timeframe=timeframe,
                        instrument_id=instrument.id,
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.5,
                        volume=1000.0,
                        timestamp=base + i * 60,
                    )
                    for i in range(20)
                ]
                db_sess.add_all(candles)
                db_sess.commit()

            client = HistoricalDataClient()
            result = list(client.fetch(
                symbol=symbol,
                market_type=market_type,
                broker_type=broker,
                timeframe=timeframe,
                start_time=base + 5 * 60,
                end_time=base + 14 * 60,
            ))

            assert len(result) == 10
            assert result[0].timestamp == base + 5 * 60
            assert result[-1].timestamp == base + 14 * 60

            with get_db_sess_sync() as db_sess:
                db_sess.execute(delete(OHLC).where(OHLC.instrument_id == instrument.id))
                db_sess.execute(delete(Instrument).where(Instrument.id == instrument.id))
                db_sess.commit()

        def test_fetch_nonexistent_instrument_returns_empty(self):
            client = HistoricalDataClient()
            result = list(client.fetch(
                symbol="DOES_NOT_EXIST_XYZ",
                market_type=MarketType.STOCKS,
                broker_type=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                start_time=1000,
                end_time=2000,
            ))

            assert len(result) == 0
