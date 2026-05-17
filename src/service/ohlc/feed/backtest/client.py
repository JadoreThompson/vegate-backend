import logging
from typing import Generator

from sqlalchemy import select

from enums import BrokerType, MarketType, Timeframe
from infra.db.model.ohlc import OHLC
from infra.db.utils import get_db_sess_sync
from models import OHLC as OHLCModel
from service.ohlc.feed.client import OHLCFeedClient


class BacktestOHLCFeedClient(OHLCFeedClient):

    def __init__(self, start: int, end: int):
        super().__init__()
        self._market_type = None
        self._symbol = None
        self._timeframe = None
        self._broker = None
        self._start = start
        self._end = end
        self._cur_candle: OHLCModel | None = None
        self._name = self.__class__.__name__
        self._logger = logging.getLogger(self._name)

    @property
    def timeframe(self):
        return self._timeframe

    @property
    def cur_candle(self) -> OHLCModel:
        return self._cur_candle

    @property
    def start(self):
        return self._start

    def subscribe(
            self,
            symbol: str,
            market_type: MarketType,
            broker: BrokerType,
            timeframe: Timeframe,
            start: int | None = None
    ) -> None:
        self._symbol = symbol
        self._market_type = market_type
        self._broker = broker
        self._timeframe = timeframe
        if start is not None:
            self._start = max(self._start, start)

        self._name = (
            f"{self.__class__.__name__}-"
            f"{market_type}-{symbol}-{timeframe}"
        )

        self._logger = logging.getLogger(self._name)

        self._logger.info(
            "Subscribed to backtest feed: "
            "symbol=%s market_type=%s broker=%s timeframe=%s start=%s",
            symbol,
            market_type,
            broker,
            timeframe,
            start,
        )

    def candles(self) -> Generator[OHLCModel, None, None]:
        with get_db_sess_sync() as db_sess:
            rows = db_sess.scalars(
                select(OHLC)
                .where(
                    OHLC.source == self._broker,
                    OHLC.symbol == self._symbol,
                    OHLC.market_type == self._market_type,
                    # OHLC.timeframe == self._timeframe,
                    OHLC.timeframe == Timeframe.m1,
                    OHLC.timestamp >= self._start,
                    OHLC.timestamp <= self._end,
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
                    broker=row.source,
                    market_type=row.market_type,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                )
                self._cur_candle = candle

                yield candle
