import asyncio
import logging
import json
from collections import defaultdict
from datetime import timedelta
from typing import Iterable

from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.data.models import Bar as AlpacaBar
from fastapi import WebSocket
from pydantic import ValidationError

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from engine.enums import BrokerType, Timeframe
from utils.redis import REDIS_CLIENT
from .models import SubscribeRequest, OHLCV


CRYPTO_SYMBOLS = ("BTC/USD",)
STOCK_SYMBOLS = ("AAPL",)

logger = logging.getLogger("bars_connection_manager")


class ConnectionManager:
    def __init__(self):
        # Broker => Symbol => Timeframe => { WebSocket, WebSocket, ... }
        self._connections: dict[
            BrokerType, dict[str, dict[Timeframe, set[WebSocket]]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        self._current_bars: dict[BrokerType, dict[str, dict[Timeframe, OHLCV]]] = (
            defaultdict(
                lambda: defaultdict(
                    lambda: {tf: None for tf in Timeframe._member_map_.values()}
                )
            )
        )
        self._alpaca_crypto_stream_client = CryptoDataStream(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)
        self._alpaca_stock_stream_client = StockDataStream(api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY)

    async def initialise(self):
        await self._restore()
        self._launch_alpaca_listener()

    def subscribe(self, ws: WebSocket, data: SubscribeRequest) -> None:
        for symbol, tf in data.symbols:
            self._connections[data.broker][symbol][tf].add(ws)

    def unsubscribe(self, ws: WebSocket, data: SubscribeRequest) -> None:
        for symbol, tf in data.symbols:
            self._connections[data.broker][symbol][tf].discard(ws)

    async def _broadcast(self, ws_sockets: Iterable[WebSocket], msg: str):
        for ws in ws_sockets:
            await ws.send_text(msg)

    async def _broadcast_proxy(
        self, redis_key: str, ws_sockets: Iterable[WebSocket], msg: str
    ):
        await REDIS_CLIENT.set(redis_key, msg)
        await self._broadcast(ws_sockets, msg)

    async def _restore(self):
        async for key in REDIS_CLIENT.scan_iter("candles:"):
            data = await REDIS_CLIENT.get(key)

            try:
                candle = OHLCV(**json.loads(data))
            except ValidationError:
                logger.info(f"Failed to restore key '{key}', value '{data}'")
                continue

            broker, symbol, tf = key.split(":", 2)
            self._current_bars[broker][symbol][tf] = candle
            logger.info(
                f"Bar for symbol '{symbol}' "
                f"at broker '{broker}' "
                f"on the '{tf}' timeframe "
                f"restored successfully"
            )

    def _launch_alpaca_listener(self):
        self._alpaca_crypto_stream_client.subscribe_bars(
            self._handle_alpaca_bar, *CRYPTO_SYMBOLS
        )
        self._alpaca_stock_stream_client.subscribe_bars(
            self._handle_alpaca_bar, *STOCK_SYMBOLS
        )
        asyncio.get_running_loop().create_task(self._alpaca_crypto_stream_client._run_forever())
        asyncio.get_running_loop().create_task(self._alpaca_stock_stream_client._run_forever())

    async def _handle_alpaca_bar(self, bar: AlpacaBar):
        broker = BrokerType.ALPACA
        symbol = bar.symbol

        conns = self._connections[broker][symbol]
        bars = self._current_bars[broker][symbol]

        coros = []

        for tf, current_bar in list(bars.items()):
            secs = tf.get_seconds()
            td_secs = timedelta(seconds=secs)

            if current_bar is None:
                if int(bar.timestamp.timestamp()) % secs == 0:
                    current_bar = OHLCV(
                        symbol=symbol,
                        timestamp=bar.timestamp,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        timeframe=tf,
                    )
                    bars[tf] = current_bar
                
                if current_bar is not None and tf == Timeframe.M1:
                    redis_key = self._get_redis_key(broker, symbol, tf)
                    coros.append(
                        (redis_key, conns[tf].copy(), bars[tf].model_dump_json())
                    )
                continue

            diff = bar.timestamp - current_bar.timestamp

            if diff < td_secs:
                current_bar.close = bar.close
                current_bar.high = max(current_bar.high, bar.close)
                current_bar.low = min(current_bar.low, bar.close)
                current_bar.volume += bar.volume
            else:
                bars[tf] = OHLCV(
                    symbol=symbol,
                    timestamp=bar.timestamp,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    timeframe=tf,
                )
                redis_key = self._get_redis_key(broker, symbol, tf)
                coros.append((redis_key, conns[tf].copy(), bars[tf].model_dump_json()))

        await asyncio.gather(
            *[
                self._broadcast_proxy(redis_key, ws_sockets, msg)
                for redis_key, ws_sockets, msg in coros
            ],
            return_exceptions=True,
        )

    @staticmethod
    def _get_redis_key(broker: BrokerType, symbol: str, timeframe: Timeframe) -> str:
        return f"candles:{broker}:{symbol}:{timeframe}"
