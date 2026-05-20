from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from typing import ClassVar

from enums import BrokerType, MarketType, Timeframe
from infra.db.model.ohlc import OHLC
from infra.db.utils import get_db_session
from models import OHLC as OHLCModel
from service.ohlc.feed.base import OHLCFeed
from service.ohlc.feed.manager import feed_manager

logger = logging.getLogger(__name__)


def _err(message: str) -> bytes:
    return (json.dumps({"type": "error", "message": message}) + "\n").encode()


def _candle_payload(candle: OHLC) -> bytes:
    """Serialise a DB OHLC row to a wire frame."""
    return (
        json.dumps(
            {
                "candle": OHLCModel(
                    open=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    timestamp=candle.timestamp,
                    timeframe=candle.timeframe,
                    symbol=candle.symbol,
                    broker=candle.source,
                    market_type=candle.market_type,
                ).model_dump(mode="json"),
                "is_live": False,
            }
        )
        + "\n"
    ).encode()


def _ohlcmodel_payload(candle: OHLCModel, is_live: bool = True) -> bytes:
    """Serialise a live OHLCModel (from a feed) to a wire frame."""
    return (
        json.dumps(
            {
                "candle": candle.model_dump(mode="json"),
                "is_live": is_live,
            }
        )
        + "\n"
    ).encode()


def _heartbeat_ack() -> bytes:
    return (json.dumps({"type": "heartbeat_ack"}) + "\n").encode()


class SocketConnection:

    def __init__(
        self,
        writer: asyncio.StreamWriter,
        symbol: str,
        market_type: MarketType,
        broker: BrokerType,
        timeframe: Timeframe,
    ):
        self.writer = writer
        self.symbol = symbol
        self.market_type = market_type
        self.broker = broker
        self.timeframe = timeframe

        # Replay state - None once bootstrapped into live stream
        self._replay_data: list[OHLC] | None = None
        self._replay_idx: int = 0

    @property
    def addr(self) -> str:
        try:
            return "{}:{}".format(*self.writer.get_extra_info("peername"))
        except Exception:
            return "<unknown>"

    def send_nowait(self, data: bytes) -> None:
        """
        Write to the transport without awaiting
        (safe from sync contexts).
        """
        self.writer.write(data)

    async def send(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    async def close(self) -> None:
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

    def __repr__(self) -> str:
        return (
            "SocketConnection("
            f"symbol={self.symbol!r}, "
            f"market_type={self.market_type!r}, "
            f"broker={self.broker!r}, "
            f"timeframe={self.timeframe!r}, "
            f"addr={self.addr!r}"
            ")"
        )


class OHLCFeedServer:
    """
    Asyncio TCP server that streams market data to internal services.
    """

    DEFAULT_HOST: ClassVar[str] = "127.0.0.1"
    DEFAULT_PORT: ClassVar[int] = 9000
    _REPLAY_PAGE: ClassVar[int] = 1_000

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
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
        Register feeds with the global feed_manager.
        """
        for feed in feeds:
            await feed_manager.register(feed)
            feed.set_on_candle(self.handle_candle)
            self._logger.info(
                "Registered feed '%s' (%s / %s / %s)",
                feed.name,
                feed.symbol,
                feed.market_type,
                feed.timeframe,
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
        await feed_manager.stop_all()
        self._logger.info("Market socket server stopped")

    async def handle_candle(self, candle: OHLCModel) -> None:
        """
        Called by a Feed whenever a new live candle is ready.
        Fans the candle out to every subscribed connection.
        """
        self._logger.info("Broadcasting candle: %s", candle)
        payload = _ohlcmodel_payload(candle)

        conns: set[SocketConnection] = self._live_conns[candle.symbol][
            candle.market_type
        ][candle.broker][candle.timeframe]

        dead: list[SocketConnection] = []
        for conn in list(conns):
            try:
                await conn.send(payload)
            except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
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

        conn: SocketConnection | None = None

        try:
            while True:
                try:
                    raw = await asyncio.wait_for(reader.readline(), timeout=30.0)
                except asyncio.TimeoutError:
                    self._logger.info("Connection %s timed out", addr)
                    break

                if not raw:
                    # EOF
                    break

                try:
                    payload = json.loads(raw.decode())
                except json.JSONDecodeError as exc:
                    writer.write(_err(f"Invalid JSON: {exc}"))
                    await writer.drain()
                    continue

                msg_type = payload.get("type")

                if msg_type == "subscribe":
                    conn = await self._handle_subscribe(payload, writer)

                elif msg_type == "replay_ack":
                    if conn is None:
                        writer.write(_err("Must subscribe before sending replay_ack"))
                        await writer.drain()
                        continue
                    await self._handle_replay_ack(conn)

                elif msg_type == "heartbeat":
                    writer.write(_heartbeat_ack())
                    await writer.drain()
                else:
                    writer.write(_err(f"Unsupported message type: '{msg_type}'"))
                    await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            self._logger.info("Connection %s closed abruptly", addr)
        except Exception as exc:
            self._logger.exception(
                "Unexpected error on connection %s", addr, exc_info=exc
            )
        finally:
            # Remove from live set if bootstrapped
            if conn is not None:
                self._live_conns[conn.symbol][conn.market_type][conn.broker][
                    conn.timeframe
                ].discard(conn)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self._logger.info("Connection %s cleaned up", addr)

    async def _handle_subscribe(
        self,
        payload: dict,
        writer: asyncio.StreamWriter,
    ) -> SocketConnection | None:
        """Validate the subscribe message, then start replay or live stream."""

        try:
            symbol: str = payload["symbol"]
            market_type = MarketType(payload["market_type"])
            broker = BrokerType(payload["broker"])
            timeframe = Timeframe(payload["timeframe"])
        except (KeyError, ValueError) as exc:
            writer.write(_err(f"Bad subscribe payload: {exc}"))
            await writer.drain()
            return None

        if symbol not in feed_manager.get_symbols():
            writer.write(_err(f"'{symbol}' is not supported"))
            await writer.drain()
            return None

        if market_type not in feed_manager.get_market_types(symbol):
            writer.write(
                _err(
                    f"Market type '{market_type}' for symbol '{symbol}' is not supported"
                )
            )
            await writer.drain()
            return None

        if broker not in feed_manager.get_brokers(symbol, market_type):
            writer.write(
                _err(
                    f"Broker '{broker}' for market type '{market_type}' "
                    f"for symbol '{symbol}' is not supported"
                )
            )
            await writer.drain()
            return None

        if timeframe not in feed_manager.get_timeframes(symbol, market_type, broker):
            writer.write(
                _err(
                    f"Timeframe '{timeframe}' for broker '{broker}' "
                    f"for market type '{market_type}' "
                    f"for symbol '{symbol}' is not supported"
                )
            )
            await writer.drain()
            return None

        conn = SocketConnection(
            writer=writer,
            symbol=symbol,
            market_type=market_type,
            broker=broker,
            timeframe=timeframe,
        )

        start: int | None = payload.get("start")

        if start is None:
            # No historical replay requested - bootstrap to live stream
            self._register_live(conn)
            return conn

        # Fetch historical page and start replay
        data = await self._fetch_ohlc(start, conn)
        if not data:
            # No historical data - bootstrap to live stream
            self._register_live(conn)
            return conn

        conn._replay_data = data
        conn._replay_idx = 0
        await self._send_next_replay_frame(conn)
        return conn

    async def _handle_replay_ack(self, conn: SocketConnection) -> None:
        """Client acknowledged the last replay frame; send the next one."""
        if conn._replay_data is None:
            return

        data = conn._replay_data
        idx = conn._replay_idx

        if idx < len(data):
            # Still within the current page
            await self._send_next_replay_frame(conn)
            return

        # Current page exhausted - try to fetch the next page
        new_start = data[-1].timestamp + conn.timeframe.get_seconds()
        new_data = await self._fetch_ohlc(new_start, conn)

        if new_data:
            conn._replay_data = new_data
            conn._replay_idx = 0
            await self._send_next_replay_frame(conn)
        else:
            # No more historical data - bootstrap to live stream
            conn._replay_data = None
            conn._replay_idx = 0
            self._register_live(conn)

    async def _send_next_replay_frame(self, conn: SocketConnection) -> None:
        """Send the candle at the current replay cursor and advance it."""
        candle = conn._replay_data[conn._replay_idx]
        conn._replay_idx += 1
        await conn.send(_candle_payload(candle=candle, is_live=False))

    def _register_live(self, conn: SocketConnection) -> None:
        self._live_conns[conn.symbol][conn.market_type][conn.broker][
            conn.timeframe
        ].add(conn)
        self._logger.info(
            "Connection %s bootstrapped into live stream (%s / %s / %s / %s)",
            conn.addr,
            conn.symbol,
            conn.market_type,
            conn.broker,
            conn.timeframe,
        )

    async def _fetch_ohlc(
        self,
        start: int,
        conn: SocketConnection,
    ) -> list[OHLC]:
        async with get_db_session() as db_sess:
            from sqlalchemy import select as sa_select

            res = await db_sess.execute(
                sa_select(OHLC)
                .where(
                    OHLC.symbol == conn.symbol,
                    OHLC.market_type == conn.market_type,
                    OHLC.source == conn.broker,
                    OHLC.timeframe == conn.timeframe,
                    OHLC.timestamp >= start,
                )
                .order_by(OHLC.timestamp.asc())
                .limit(self._REPLAY_PAGE)
            )
            return res.scalars().all()
