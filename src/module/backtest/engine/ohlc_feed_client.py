import logging
from typing import Generator

from sqlalchemy import and_, or_, select

from core.db import get_db_sess_sync
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.feed.client import OHLCFeedClient
from vegate.markets.feed.schema import SubscribeRequest
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.oms.enums import BrokerType
from module.markets.model import Instrument, OHLC


class BacktestOHLCFeedClient(OHLCFeedClient):

    def __init__(self, start: int, end: int):
        super().__init__()
        self._subscriptions: list[dict] = []
        self._start = start
        self._end = end
        self._cur_candle: OHLCSchema | None = None
        self._name = self.__class__.__name__
        self._logger = logging.getLogger(self._name)

    @property
    def cur_candle(self) -> OHLCSchema:
        return self._cur_candle

    @property
    def start(self):
        return self._start

    def subscribe(self, instruments: list[dict]) -> None:
        for instrument in instruments:
            SubscribeRequest.model_validate(instrument)
            
        self._subscriptions = instruments
        self._logger.info(
            "Subscribed to backtest feed: %d instrument(s)",
            len(self._subscriptions),
        )

    def candles(self) -> Generator[OHLCSchema, None, None]:
        last_timestamp = None
        while last_timestamp is None or last_timestamp < self._end:
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
                        (
                            (OHLC.timestamp > last_timestamp)
                            if last_timestamp is not None
                            else (OHLC.timestamp >= self._start)
                        ),
                        OHLC.timestamp <= self._end,
                    )
                    .order_by(OHLC.timestamp.asc())
                )

                prev_symbols = set(
                    [subscription["symbol"] for subscription in self._subscriptions]
                )
                last_row = None

                for row in rows.yield_per(1000):
                    new_symbols = set(
                        [subscription["symbol"] for subscription in self._subscriptions]
                    )
                    if new_symbols != prev_symbols:
                        self._logger.info(
                            "Subscription changed during candle retrieval. Restarting."
                        )
                        break
                    
                    last_row = row
                    ohlc, instrument = row.tuple()
                    last_timestamp = ohlc.timestamp
                    candle = OHLCSchema(
                        open=ohlc.open,
                        high=ohlc.high,
                        low=ohlc.low,
                        close=ohlc.close,
                        volume=ohlc.volume,
                        symbol=instrument.native_symbol,
                        broker=instrument.broker_type,
                        market_type=instrument.market_type,
                        timeframe=ohlc.timeframe,
                        timestamp=ohlc.timestamp,
                    )
                    self._cur_candle = candle

                    yield candle
                
                if last_row is None:
                    break
