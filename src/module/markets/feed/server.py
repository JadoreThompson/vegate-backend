from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from pydantic import ValidationError

from vegate.oms.enums import BrokerType
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.markets.feed.schema import SubscribeRequest
from .base import OHLCFeed
from .manager import FeedManager

logger = logging.getLogger(__name__)


class SocketConnection:
    """A socket connection with an internal async queue so that producers
    can push candle data without blocking, while a background task drains
    the queue and writes to the transport concurrently."""

    _SEND_QUEUE_MAXSIZE = 1024

    def __init__(self, writer: asyncio.StreamWriter):
        self._writer = writer
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue(
            maxsize=self._SEND_QUEUE_MAXSIZE
        )
        self._send_task: asyncio.Task | None = None
        self._dead = False

    @property
    def writer(self) -> asyncio.StreamWriter:
        return self._writer

    @property
    def addr(self) -> str:
        try:
            return "{}:{}".format(*self.writer.get_extra_info("peername"))
        except Exception:
            return "<unknown>"

    @property
    def alive(self) -> bool:
        return not self._dead

    def start_sender(self) -> None:
        """Launch the background send-loop task."""
        if self._send_task is None or self._send_task.done():
            self._send_task = asyncio.create_task(self._send_loop())

    async def _send_loop(self) -> None:
        """Continuously pull items from the queue and write to the transport."""
        try:
            while True:
                data = await self._queue.get()
                if data is None:
                    self._queue.task_done()
                    return
                try:
                    self._writer.write(data)
                    await self._writer.drain()
                except (ConnectionResetError, BrokenPipeError, OSError):
                    self._dead = True
                    return
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except asyncio.QueueEmpty:
                    break
            raise

    def send_nowait(self, data: bytes) -> bool:
        """
        Push data to the send queue without awaiting.
        Returns True on success, False if the queue was full or the
        connection is no longer alive.
        """
        if not self.alive:
            return False
        try:
            self._queue.put_nowait(data)
            return True
        except asyncio.QueueFull:
            return False

    async def send(self, data: bytes) -> None:
        """Push data to the send queue, awaiting if the queue is full."""
        if not self.alive:
            raise ConnectionResetError("Connection is no longer alive")
        await self._queue.put(data)

    async def close(self) -> None:
        self._dead = True
        if self._send_task is not None and not self._send_task.done():
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                self._send_task.cancel()
            try:
                await asyncio.wait_for(self._send_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._send_task.cancel()
                try:
                    await self._send_task
                except (asyncio.CancelledError, Exception):
                    pass
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass


class OHLCFeedServer:

    def __init__(
        self,
        feed_manager: FeedManager,
        host: str = "127.0.0.1",
        port: int = 9000,
    ) -> None:
        self._feed_manager = feed_manager
        self._host = host
        self._port = port

        # symbol -> market_type -> broker -> timeframe -> {SocketConnection}
        self._live_conns: dict[
            str,
            dict[MarketType, dict[BrokerType, dict[Timeframe, set[SocketConnection]]]],
        ] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        )

        self._server: asyncio.Server | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def init(self, feeds: list[OHLCFeed]) -> None:
        """
        Register feeds with the global self._feed_manager.
        """
        for feed in feeds:
            feed.set_on_candle(self.handle_candle)
            await self._feed_manager.register(feed)
            self._logger.info(
                "Registered feed '%s' (%s timeframes=%s)",
                feed.name,
                feed.market_type,
                feed.timeframes,
            )

    async def run(self) -> None:
        """Start listening for incoming socket connections."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self._host,
            self._port,
        )
        addr = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        self._logger.info("Market socket server listening on %s", addr)
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Gracefully shut down the server and all active feeds."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        self._logger.info("Market socket server stopped")

    async def handle_candle(self, candle: OHLCSchema) -> None:
        """
        Called by a Feed whenever a new live candle is ready.
        Pushes the candle into each subscribed connection's queue so that
        every connection receives it concurrently via its own background
        send-loop task.
        """
        self._logger.info("Broadcasting candle: %s", candle)
        payload = self._ohlcmodel_payload(candle)

        conns: set[SocketConnection] = self._live_conns[candle.symbol][
            candle.market_type
        ][candle.broker][candle.timeframe]

        dead: list[SocketConnection] = []
        for conn in list(conns):
            if not conn.send_nowait(payload):
                dead.append(conn)

        for conn in dead:
            conns.discard(conn)
            self._logger.warning("Dropped dead connection %s", conn.addr)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        addr = "{}:{}".format(*writer.get_extra_info("peername"))
        self._logger.info("New connection from %s", addr)

        conn = SocketConnection(writer)
        conn.start_sender()
        instruments: set[tuple[str, MarketType, BrokerType, Timeframe]] = set()

        try:
            while True:
                try:
                    raw = await asyncio.wait_for(reader.readline(), timeout=30.0)
                except asyncio.TimeoutError:
                    self._logger.info("Connection %s timed out", addr)
                    break

                if not raw:
                    break

                try:
                    payload = json.loads(raw.decode())
                except json.JSONDecodeError as exc:
                    await conn.send(self._err(f"Invalid JSON"))
                    continue
                msg_type = payload.get("type")

                if msg_type == "subscribe":
                    result = await self._handle_subscribe(payload, conn, instruments)
                    if result is not None:
                        instruments = result

                elif msg_type == "heartbeat":
                    await conn.send(self._heartbeat_ack())
                else:
                    await conn.send(
                        self._err(f"Unsupported message type: '{msg_type}'")
                    )

        except (ConnectionResetError, BrokenPipeError):
            self._logger.info("Connection %s closed abruptly", addr)
        except Exception as exc:
            self._logger.exception(
                "Unexpected error on connection %s", addr, exc_info=exc
            )
        finally:
            for key in instruments:
                symbol, market_type, broker_type, timeframe = key
                self._live_conns[symbol][market_type][broker_type][timeframe].discard(
                    conn
                )

            try:
                await conn.close()
            except Exception:
                pass

            self._logger.info("Connection %s cleaned up", addr)

    async def _handle_subscribe(
        self,
        payload: dict,
        conn: SocketConnection,
        existing_instruments: set[tuple[str, MarketType, BrokerType, Timeframe]],
    ):
        """Validate the subscribe message and register live connections."""

        try:
            instruments = payload["instruments"]
        except KeyError:
            await conn.send(self._err("Missing 'instruments' in subscribe payload"))
            return None

        if not isinstance(instruments, list) or not instruments:
            await conn.send(self._err("'instruments' must be a non-empty list"))
            return None

        expanded: list[tuple[str, MarketType, BrokerType, Timeframe]] = []

        for idx, item in enumerate(instruments):
            try:
                request = SubscribeRequest.model_validate(item)
            except ValidationError as exc:
                await conn.send(
                    self._err(f"Bad instrument entry at index {idx}: {exc}")
                )
                return None

            symbol = request.symbol
            market_type = request.market_type
            broker_type = request.broker_type

            if symbol not in self._feed_manager.get_symbols():
                await conn.send(
                    self._err(f"'{symbol}' at index {idx} is not supported")
                )
                return None

            if market_type not in self._feed_manager.get_market_types(symbol):
                await conn.send(
                    self._err(
                        f"Market type '{market_type}' for symbol '{symbol}' "
                        f"at index {idx} is not supported"
                    )
                )
                return None

            if broker_type not in self._feed_manager.get_brokers(symbol, market_type):
                await conn.send(
                    self._err(
                        f"Broker '{broker_type}' for market type '{market_type}' "
                        f"for symbol '{symbol}' at index {idx} is not supported"
                    )
                )
                return None

            for timeframe in request.timeframe:
                if timeframe not in self._feed_manager.get_timeframes(
                    symbol, market_type, broker_type
                ):
                    await conn.send(
                        self._err(
                            f"Timeframe '{timeframe}' for broker '{broker_type}' "
                            f"for market type '{market_type}' "
                            f"for symbol '{symbol}' at index {idx} is not supported"
                        )
                    )
                    return None

                expanded.append((symbol, market_type, broker_type, timeframe))

        instruments = set()
        for symbol, market_type, broker_type, timeframe in expanded:
            key = (symbol, market_type, broker_type, timeframe)
            if key not in existing_instruments:
                self._register_live(symbol, market_type, broker_type, timeframe, conn)
            instruments.add(key)

        for key in existing_instruments:
            if key not in instruments:
                symbol, market_type, broker_type, timeframe = key
                self._live_conns[symbol][market_type][broker_type][timeframe].discard(
                    conn
                )
                self._logger.info(
                    "Unsubscribed connection %s from live stream (%s / %s / %s / %s)",
                    conn.addr,
                    symbol,
                    market_type,
                    broker_type,
                    timeframe,
                )

        return instruments

    def _register_live(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        conn: SocketConnection,
    ) -> None:
        self._live_conns[symbol][market_type][broker_type][timeframe].add(conn)
        self._logger.info(
            "Connection %s bootstrapped into live stream (%s / %s / %s / %s)",
            conn.addr,
            symbol,
            market_type,
            broker_type,
            timeframe,
        )

    def _err(self, message: str) -> bytes:
        return self._create_message({"type": "error", "message": message})

    def _ohlcmodel_payload(self, candle: OHLCSchema, is_live: bool = True) -> bytes:
        """Serialise a live OHLCSchema (from a feed) to a wire frame."""
        return self._create_message(
            {"candle": candle.model_dump(mode="json"), "is_live": is_live}
        )

    def _heartbeat_ack(self) -> bytes:
        return self._create_message({"type": "heartbeat_ack"})

    def _create_message(self, payload: dict):
        return (json.dumps(payload) + "\n").encode()
