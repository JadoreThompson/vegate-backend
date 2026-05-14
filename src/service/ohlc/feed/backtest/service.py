import logging
from datetime import datetime
from typing import Generator

from sqlalchemy import select

from enums import BrokerType, MarketType, Timeframe
from infra.db.model.ohlc import OHLC
from infra.db.utils import get_db_sess_sync
from models import OHLC as OHLCModel
from service.ohlc.feed.base import OHLCFeed


class BacktestOHLCFeed(OHLCFeed):

    def __init__(
        self,
        market_type: MarketType,
        symbol: str,
        timeframe: Timeframe,
        broker: BrokerType,
        start_date: datetime,
        end_date: datetime,
    ):
        self._market_type = market_type
        self._symbol = symbol
        self._timeframe = timeframe
        self._broker = broker
        self._start_date = start_date
        self._end_date = end_date

        self._name = f"{self.__class__.__name__}-{market_type}-{symbol}"
        self._logger = logging.getLogger(self._name)

    @property
    def name(self):
        return self._name

    @property
    def market_type(self):
        return self._market_type

    @property
    def symbol(self):
        return self._symbol

    @property
    def broker(self):
        return self._broker

    @property
    def timeframe(self):
        return self._timeframe

    def __iter__(self) -> Generator[OHLCModel, None, None]:
        with get_db_sess_sync() as db_sess:
            rows = db_sess.scalars(
                select(OHLC)
                .where(
                    OHLC.source == self._broker,
                    OHLC.symbol == self._symbol,
                    OHLC.market_type == self._market_type,
                    OHLC.timeframe == self._timeframe,
                    OHLC.timestamp >= int(self._start_date.timestamp()),
                    OHLC.timestamp <= int(self._end_date.timestamp()),
                )
                .order_by(OHLC.timestamp.asc())
            )

            for row in rows.yield_per(1000):
                candle = OHLCModel(
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    volume=float(row.volume),
                    symbol=row.symbol,
                    market_type=row.market_type,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                )

                yield candle
