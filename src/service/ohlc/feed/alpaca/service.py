import asyncio
from datetime import datetime, timedelta
import json
import logging
from asyncio import iscoroutine
from typing import Any, Awaitable, Callable
from uuid import UUID

from sqlalchemy import insert
import websockets

from enums import BrokerType, MarketType, Timeframe
from infra.db.model.ohlc import OHLC
from infra.db.utils import get_db_session
from models import OHLC as OHLCModel
from service.ohlc.feed.alpaca.exception import AlpacaFeedException
from service.ohlc.feed.base import OHLCFeed
from service.ohlc.loader.alpaca import AlpacaOHLCLoader
from utils import get_datetime


class AlpacaOHLCFeed(OHLCFeed):
    """
    Docs: https://docs.alpaca.markets/us/docs/streaming-market-data
    """

    def __init__(
        self,
        market_type: MarketType,
        symbol: str,
        timeframe: Timeframe,
        api_key: str,
        secret_key: str,
        start_date: datetime,
    ):
        self._market_type = market_type
        self._symbol = symbol
        self._fmt_symbol = self._symbol.replace("/", "")
        self._timeframe = timeframe
        self._api_key = api_key
        self._secret_key = secret_key
        self._start_date = start_date

        self._on_candle = None
        self._instrument_id: UUID | None = None
        self._task: asyncio.Task | None = None
        self._name = f"{self.__class__.__name__}-{market_type}-{self._fmt_symbol}"

        self._logger = logging.getLogger(self._name)

    @property
    def name(self):
        return self._name

    @property
    def market_type(self):
        return self._market_type

    @property
    def symbol(self):
        # return self._fmt_symbol
        return self._symbol

    @property
    def broker(self):
        return BrokerType.ALPACA

    @property
    def timeframe(self):
        return self._timeframe

    async def run(self) -> None:
        loader = AlpacaOHLCLoader(self._api_key, self._secret_key)
        result = await loader.load_candles(
            symbol=self._symbol,
            market_type=self.market_type,
            timeframe=self._timeframe,
            start_date=self._start_date,
            end_date=get_datetime().date() + timedelta(days=1),
        )
        self._instrument_id = loader.instrument_id

        url = (
            "wss://stream.data.alpaca.markets/v1beta3/crypto/eu-1"
            if self._market_type == MarketType.CRYPTO
            else "wss://stream.data.alpaca.markets/v2/iex"
        )

        async with websockets.connect(url) as ws:
            await ws.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": self._api_key,
                        "secret": self._secret_key,
                    }
                )
            )
            resp = await ws.recv()

            await ws.send(json.dumps(self._generate_subscription_message()))
            resp = await ws.recv()
            self._logger.info(f"Subscribe response: {resp}")

            # Skipping confirmation
            msg = await ws.recv()
            data = json.loads(msg)
            if data[0]["T"] == "error":
                raise AlpacaFeedException(f"Received error: {data}")

            while True:
                msg = await ws.recv()
                self._logger.info(msg)
                data = json.loads(msg)
                if self._on_candle is not None:
                    candle_data = data[0]
                    candle = OHLCModel(
                        open=candle_data["o"],
                        high=candle_data["h"],
                        low=candle_data["l"],
                        close=candle_data["c"],
                        # symbol=self._fmt_symbol,
                        symbol=self._symbol,
                        volume=candle_data["v"],
                        broker=BrokerType.ALPACA,
                        market_type=self._market_type,
                        timestamp=int(datetime.fromisoformat(candle_data["t"]).timestamp()),
                        timeframe=self._timeframe,
                    )
                    await self._persist_candle(candle)
                    res = self._on_candle(candle)
                    if iscoroutine(res):
                        await res

    async def join(self) -> None:
        await self._task

    async def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _persist_candle(self, candle: OHLCModel) -> None:
        async with get_db_session() as db_sess:
            await db_sess.execute(
                insert(OHLC).values(
                    # source=BrokerType.ALPACA,
                    # symbol=candle.symbol,
                    # market_type=candle.market_type,
                    instrument_id=self._instrument_id,
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    timeframe=candle.timeframe,
                    timestamp=candle.timestamp,
                )
            )
            await db_sess.commit()

    def _generate_subscription_message(self) -> dict[str, Any]:
        payload = {"action": "subscribe"}
        if self._timeframe.get_seconds() < 86_400:
            payload["bars"] = [self._symbol]
        else:
            payload["dailyBars"] = [self._symbol]

        return payload

    def set_on_candle(self, func: Callable[[OHLC], Any | Awaitable[Any]]) -> None:
        self._on_candle = func
