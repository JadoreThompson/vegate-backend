import logging
from typing import Generator

from sqlalchemy import select

from core.db import get_db_sess_sync
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.model import Instrument, OHLC
from module.markets.schema import OHLC as OHLCSchema


class HistoricalDataClient:

    def __init__(self):
        self._name = self.__class__.__name__
        self._logger = logging.getLogger(self._name)

    def fetch(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        start_time: int,
        end_time: int,
    ) -> Generator[OHLCSchema, None, None]:
        with get_db_sess_sync() as db_sess:
            instrument_subq = (
                select(Instrument.id)
                .where(
                    Instrument.symbol == symbol,
                    Instrument.market_type == market_type,
                    Instrument.broker_type == broker_type,
                )
                .subquery()
            )

            rows = db_sess.execute(
                select(OHLC, Instrument)
                .where(
                    Instrument.id == instrument_subq.c.id,
                    OHLC.timeframe == timeframe,
                    OHLC.timestamp >= start_time,
                    OHLC.timestamp <= end_time,
                )
                .order_by(OHLC.timestamp.asc())
            )

            for row in rows.yield_per(1000):
                ohlc, instrument = row.tuple()
                candle = OHLCSchema(
                    open=ohlc.open,
                    high=ohlc.high,
                    low=ohlc.low,
                    close=ohlc.close,
                    volume=ohlc.volume,
                    symbol=instrument.symbol,
                    broker=instrument.broker_type,
                    market_type=instrument.market_type,
                    timeframe=ohlc.timeframe,
                    timestamp=ohlc.timestamp,
                )
                yield candle
