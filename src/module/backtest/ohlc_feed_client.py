import logging
from typing import Generator

from sqlalchemy import and_, or_, select

from core.db import get_db_sess_sync
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.feed import OHLCFeedClient
from module.markets.model import Instrument, OHLC
from module.markets.schema import OHLC as OHLCSchema


class BacktestOHLCFeedClient(OHLCFeedClient):

    def __init__(self, start: int, end: int):
        super().__init__()
        self._subscriptions: list[dict] = []
        self._timeframe = None
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

    def subscribe(self, instruments: list[dict]) -> None:
        self._subscriptions = []
        for inst in instruments:
            symbol = inst["symbol"]
            market_type = inst["market_type"]
            broker_type = inst["broker_type"]
            timeframes = inst.get("timeframe", [])
            if isinstance(timeframes, (Timeframe, str)):
                timeframes = [timeframes]
                
            for tf in timeframes:
                self._subscriptions.append(
                    {
                        "symbol": symbol,
                        "market_type": (
                            market_type.value
                            if isinstance(market_type, MarketType)
                            else market_type
                        ),
                        "broker_type": (
                            broker_type.value
                            if isinstance(broker_type, BrokerType)
                            else broker_type
                        ),
                        "timeframe": tf.value if isinstance(tf, Timeframe) else tf,
                    }
                )

        if self._subscriptions:
            first = self._subscriptions[0]
            self._timeframe = Timeframe(first["timeframe"])
            self._name = (
                f"{self.__class__.__name__}-"
                f"{first['market_type']}-{first['symbol']}-{first['timeframe']}"
            )
        else:
            self._timeframe = None
            self._name = self.__class__.__name__

        self._logger = logging.getLogger(self._name)

        self._logger.info(
            "Subscribed to backtest feed: %d instrument(s)",
            len(self._subscriptions),
        )

    def candles(self) -> Generator[OHLCSchema, None, None]:
        with get_db_sess_sync() as db_sess:
            filters = []
            for sub in self._subscriptions:
                filters.append(
                    and_(
                        Instrument.native_symbol == sub["symbol"],
                        Instrument.market_type == MarketType(sub["market_type"]),
                        Instrument.broker_type == BrokerType(sub["broker_type"]),
                    )
                )

            squery = (
                select(Instrument.id)
                .where(or_(*filters) if filters else False)
                .subquery()
            )

            rows = db_sess.execute(
                select(OHLC, Instrument)
                .where(
                    Instrument.id == squery.c.id,
                    OHLC.instrument_id == Instrument.id,
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
