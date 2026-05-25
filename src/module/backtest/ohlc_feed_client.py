import logging
from typing import Generator

from sqlalchemy import select

from core.db import get_db_sess_sync
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.feed import OHLCFeedClient
from module.markets.model import Instrument, OHLC
from module.markets.schema import OHLC as OHLCSchema


class BacktestOHLCFeedClient(OHLCFeedClient):

    def __init__(self, start: int, end: int):
        super().__init__()
        self._market_type = None
        self._symbol = None
        self._timeframe = None
        self._broker_type = None
        self._start = start
        self._end = end
        self._cur_candle: OHLCSchema | None = None
        self._name = self.__class__.__name__
        self._logger = logging.getLogger(self._name)

    @property
    def timeframe(self):
        return self._timeframe

    @property
    def cur_candle(self) -> OHLCSchema:
        return self._cur_candle

    @property
    def start(self):
        return self._start

    def subscribe(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
    ) -> None:
        self._symbol = symbol
        self._market_type = market_type
        self._broker_type = broker_type
        self._timeframe = timeframe

        self._name = (
            f"{self.__class__.__name__}-"
            f"{market_type.value}-{symbol}-{timeframe.value}"
        )

        self._logger = logging.getLogger(self._name)

        self._logger.info(
            "Subscribed to backtest feed: "
            "symbol=%s market_type=%s broker=%s timeframe=%s",
            symbol,
            market_type,
            broker_type,
            timeframe,
        )

    def candles(self) -> Generator[OHLCSchema, None, None]:
        with get_db_sess_sync() as db_sess:
            squery = (
                select(Instrument.id)
                .where(
                    Instrument.native_symbol == self._symbol,
                    Instrument.market_type == self._market_type,
                    Instrument.broker_type == self._broker_type,
                )
                .subquery()
            )
            rows = db_sess.execute(
                select(OHLC, Instrument)
                .where(
                    Instrument.id == squery.c.id,
                    OHLC.timeframe == Timeframe.m1,
                    OHLC.timestamp >= self._start,
                    OHLC.timestamp <= self._end,
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
                self._cur_candle = candle

                yield candle
