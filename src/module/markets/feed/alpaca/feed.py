import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable
from uuid import UUID

import websockets
from sqlalchemy import insert, select

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from core.db import get_db_session
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.oms.enums import BrokerType
from .exception import AlpacaOHLCFeedException
from ..base import OHLCFeed
from ..exception import MaxRetryAttemptsException
from ...model import OHLC, Instrument


class AlpacaOHLCFeed(OHLCFeed):
    """
    Docs: https://docs.alpaca.markets/us/docs/streaming-market-data
    """

    def __init__(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        api_key: str = ALPACA_API_KEY,
        secret_key: str = ALPACA_SECRET_KEY,
        retry_attempts = 5,
        retry_delay = 10,
    ):
        self._market_type = market_type
        self._symbol = symbol
        self._fmt_symbol = self._symbol.replace("/", "")
        self._timeframe = timeframe
        self._api_key = api_key
        self._secret_key = secret_key
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay

        self._on_candle = None
        self._instrument_id: UUID | None = None
        self._task: asyncio.Task | None = None
        self._name = f"{self.__class__.__name__}-{self._fmt_symbol}-{market_type.value}"

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
        return BrokerType.ALPACA

    @property
    def timeframe(self):
        return self._timeframe

    async def run(self) -> None:
        url = self._get_url()
        self._instrument_id = await self._get_or_create_instrument_id()

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
            msg = await ws.recv()

            connected = False
            for _ in range(self._retry_attempts):
                await ws.send(json.dumps(self._generate_subscription_message()))
                msg = await ws.recv()
                payload = json.loads(msg)
                self._logger.info(f"Subscribe response: {msg}")
                
                if payload[0]["T"] == "success":
                    connected = True
                    break
                    
                await asyncio.sleep(self._retry_delay)
                    
            if not connected:
                self._logger.warning("Failed to connect. Aborting run.")
                raise MaxRetryAttemptsException()

            # Skipping confirmation
            msg = await ws.recv()
            data = json.loads(msg)
            if data[0]["T"] == "error":
                raise AlpacaOHLCFeedException(f"Received error: {data}")

            while True:
                msg = await ws.recv()
                self._logger.info("Received message %s", msg)
                data = json.loads(msg)
                bar = data[0]
                candle = self._parse_candle(bar)

                await self._persist_candle(candle)

                if self._on_candle is not None:
                    res = self._on_candle(candle)
                    if asyncio.iscoroutine(res):
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

    async def _get_or_create_instrument_id(self) -> UUID:
        """Fetch existing instrument or create a new one."""
        async with get_db_session() as db_sess:
            instrument_id = await db_sess.scalar(
                select(Instrument.id).where(
                    Instrument.native_symbol == self._symbol,
                    Instrument.market_type == self._market_type,
                    Instrument.broker_type == BrokerType.ALPACA,
                )
            )
            if instrument_id is not None:
                return instrument_id

            instrument_id = await db_sess.scalar(
                insert(Instrument)
                .values(
                    symbol=self._format_symbol(self._symbol),
                    native_symbol=self._symbol,
                    market_type=self._market_type,
                    broker_type=BrokerType.ALPACA,
                )
                .returning(Instrument.id)
            )
            await db_sess.commit()
            return instrument_id

    async def _persist_candle(self, candle: OHLCSchema) -> None:
        async with get_db_session() as db_sess:
            await db_sess.execute(
                insert(OHLC).values(
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

    def _get_url(self):
        if self._market_type == MarketType.CRYPTO:
            return "wss://stream.data.alpaca.markets/v1beta3/crypto/eu-1"
        return "wss://stream.data.alpaca.markets/v2/iex"
    
    def _format_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "")

    def _generate_subscription_message(self) -> dict[str, Any]:
        payload = {"action": "subscribe"}
        if self._timeframe.get_seconds() < 86_400:
            payload["bars"] = [self._symbol]
        else:
            payload["dailyBars"] = [self._symbol]

        return payload

    def set_on_candle(self, func: Callable[[OHLC], Any | Awaitable[Any]]) -> None:
        self._on_candle = func

    def _parse_candle(self, candle: dict):
        return OHLCSchema(
            open=candle["o"],
            high=candle["h"],
            low=candle["l"],
            close=candle["c"],
            symbol=self._symbol,
            volume=candle["v"],
            broker=BrokerType.ALPACA,
            market_type=self._market_type,
            timestamp=int(datetime.fromisoformat(candle["t"]).timestamp()),
            timeframe=self._timeframe,
        )
