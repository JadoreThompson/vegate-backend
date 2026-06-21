import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable

import websockets

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC
from vegate.oms.enums import BrokerType
from .exception import AlpacaOHLCFeedException
from ..base import OHLCFeed
from ..exception import MaxRetryAttemptsException
from ...aggregator import CandleAggregator


class AlpacaOHLCFeed(OHLCFeed):

    def __init__(
        self,
        market_type: MarketType,
        instruments: list[tuple[str, list[Timeframe]]],
        api_key: str = ALPACA_API_KEY,
        secret_key: str = ALPACA_SECRET_KEY,
        retry_attempts: int = 5,
        retry_delay: int = 10,
    ):
        self._market_type = market_type
        self._instruments = instruments
        self._api_key = api_key
        self._secret_key = secret_key
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay

        self._on_candle: Callable[[OHLC], Any] | None = None
        self._ws: websockets.ClientConnection | None = None
        self._stopped = False
        self._name = (
            f"{self.__class__.__name__}-{market_type.value}-{len(instruments)}sym"
        )

        self._logger = logging.getLogger(self._name)

        # Determine the lowest common timeframe.
        all_tfs = {tf for _, tfs in instruments for tf in tfs}
        self._lowest_tf = min(all_tfs, key=lambda tf: tf.get_seconds())
        self._all_symbols = [sym for sym, _ in instruments]

        # Build a lookup: symbol -> list[Timeframe]
        self._sym_tfs: dict[str, list[Timeframe]] = {
            sym: tfs for sym, tfs in instruments
        }

        # Create aggregators for every (symbol, timeframe) above the lowest.
        self._aggregators: dict[tuple[str, Timeframe], CandleAggregator] = {}
        for sym, tfs in instruments:
            for tf in tfs:
                if tf != self._lowest_tf:
                    self._aggregators[(sym, tf)] = CandleAggregator(tf)

    @property
    def name(self) -> str:
        return self._name

    @property
    def market_type(self) -> MarketType:
        return self._market_type

    @property
    def symbols(self) -> list[str]:
        return self._all_symbols

    @property
    def broker(self) -> BrokerType:
        return BrokerType.ALPACA

    @property
    def timeframes(self) -> list[Timeframe]:
        return list({tf for _, tfs in self._instruments for tf in tfs})

    async def run(self) -> None:
        self._stopped = False
        url = self._get_url()

        self._ws = await websockets.connect(url)
        try:
            await self._ws.send(
                json.dumps(
                    {
                        "action": "auth",
                        "key": self._api_key,
                        "secret": self._secret_key,
                    }
                )
            )
            msg = await self._ws.recv()

            connected = False
            for _ in range(self._retry_attempts):
                await self._ws.send(json.dumps(self._generate_subscription_message()))
                msg = await self._ws.recv()
                payload = json.loads(msg)
                self._logger.info("Subscribe response: %s", msg)

                if payload[0]["T"] == "success":
                    connected = True
                    break

                await asyncio.sleep(self._retry_delay)

            if not connected:
                self._logger.warning("Failed to connect. Aborting run.")
                raise MaxRetryAttemptsException()

            msg = await self._ws.recv()
            data = json.loads(msg)
            if data[0]["T"] == "error":
                raise AlpacaOHLCFeedException(f"Received error: {data}")

            while not self._stopped:
                msg = await self._ws.recv()
                self._logger.info("Received message %s", msg)
                data = json.loads(msg)
                bar = data[0]
                await self._process_bar(bar)

        except Exception:
            if self._stopped:
                return
            raise
        finally:
            await self._close_ws()

    async def stop(self) -> None:
        self._stopped = True
        await self._close_ws()

    async def _close_ws(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    def set_on_candle(self, func: Callable[[OHLC], Any | Awaitable[Any]]) -> None:
        self._on_candle = func

    async def _process_bar(self, raw: dict[str, Any]) -> None:
        sym = raw["S"]
        lowest_schema = self._raw_to_schema(raw, self._lowest_tf, sym)

        # Emit lowest tf
        if self._lowest_tf in self._sym_tfs.get(sym, []):
            await self._emit(lowest_schema)

        # Feed aggregators
        for tf in self._sym_tfs.get(sym, []):
            if tf == self._lowest_tf:
                continue

            agg = self._aggregators.get((sym, tf))
            if agg is None:
                continue

            completed = agg.add_bar(lowest_schema)
            if completed is not None:
                await self._emit(completed)

    async def _emit(self, candle: OHLC) -> None:
        if self._on_candle is not None:
            res = self._on_candle(candle)
            if asyncio.iscoroutine(res):
                await res

    def _generate_subscription_message(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"action": "subscribe"}
        if self._lowest_tf.get_seconds() < 86_400:
            payload["bars"] = self._all_symbols
        else:
            payload["dailyBars"] = self._all_symbols
        return payload

    def _get_url(self) -> str:
        if self._market_type == MarketType.CRYPTO:
            return "wss://stream.data.alpaca.markets/v1beta3/crypto/eu-1"
        return "wss://stream.data.alpaca.markets/v2/iex"

    def _raw_to_schema(
        self, raw: dict[str, Any], timeframe: Timeframe, symbol: str
    ) -> OHLC:
        return OHLC(
            open=raw["o"],
            high=raw["h"],
            low=raw["l"],
            close=raw["c"],
            volume=raw["v"],
            symbol=symbol,
            broker=BrokerType.ALPACA,
            market_type=self._market_type,
            timestamp=int(datetime.fromisoformat(raw["t"]).timestamp()),
            timeframe=timeframe,
        )

    def _format_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "")
