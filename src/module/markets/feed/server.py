from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict

from module.broker.enums import BrokerType
from .base import OHLCFeed
from .manager import feed_manager
from ..enums import MarketType, Timeframe
from ..schema import OHLC as OHLCSchema

logger = logging.getLogger(__name__)


class SocketConnection:

    def __init__(
        self,
        writer: asyncio.StreamWriter,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
    ):
        self.writer = writer
        self.symbol = symbol
        self.market_type = market_type
        self.broker_type = broker_type
        self.timeframe = timeframe

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
            f"broker_type={self.broker_type!r}, "
            f"timeframe={self.timeframe!r}, "
            f"addr={self.addr!r}"
            ")"
        )


class OHLCFeedServer:

    def __init__(self, host: str = "127.0.0.1", port: int = 9000) -> None:
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
            feed.set_on_candle(self.handle_candle)
            await feed_manager.register(feed)
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
        self._logger.info("Market socket server stopped")

    async def handle_candle(self, candle: OHLCSchema) -> None:
        """
        Called by a Feed whenever a new live candle is ready.
        Fans the candle out to every subscribed connection.
        """
        self._logger.info("Broadcasting candle: %s", candle)
        payload = self._ohlcmodel_payload(candle)

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

        conns: list[SocketConnection] = []

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
                    writer.write(self._err(f"Invalid JSON"))
                    await writer.drain()
                    continue

                msg_type = payload.get("type")

                if msg_type == "subscribe":
                    result = await self._handle_subscribe(payload, writer)
                    if result is not None:
                        conns.extend(result)

                elif msg_type == "heartbeat":
                    writer.write(self._heartbeat_ack())
                    await writer.drain()
                else:
                    writer.write(self._err(f"Unsupported message type: '{msg_type}'"))
                    await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            self._logger.info("Connection %s closed abruptly", addr)
        except Exception as exc:
            self._logger.exception(
                "Unexpected error on connection %s", addr, exc_info=exc
            )
        finally:
            for conn in conns:
                self._live_conns[conn.symbol][conn.market_type][conn.broker_type][
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
    ) -> list[SocketConnection] | None:
        """Validate the subscribe message and register live connections."""

        try:
            instruments = payload["instruments"]
        except KeyError:
            writer.write(self._err("Missing 'instruments' in subscribe payload"))
            await writer.drain()
            return None

        if not isinstance(instruments, list) or not instruments:
            writer.write(self._err("'instruments' must be a non-empty list"))
            await writer.drain()
            return None

        expanded: list[tuple[str, MarketType, BrokerType, Timeframe]] = []

        for idx, item in enumerate(instruments):
            try:
                symbol: str = item["symbol"]
                market_type = MarketType(item["market_type"])
                broker_type = BrokerType(item["broker_type"])
                raw_timeframes = item.get("timeframe", [])
                if isinstance(raw_timeframes, str):
                    raw_timeframes = [raw_timeframes]
            except (KeyError, ValueError) as exc:
                writer.write(self._err(f"Bad instrument entry at index {idx}: {exc}"))
                await writer.drain()
                return None

            if symbol not in feed_manager.get_symbols():
                writer.write(self._err(f"'{symbol}' at index {idx} is not supported"))
                await writer.drain()
                return None

            if market_type not in feed_manager.get_market_types(symbol):
                writer.write(
                    self._err(
                        f"Market type '{market_type}' for symbol '{symbol}' "
                        f"at index {idx} is not supported"
                    )
                )
                await writer.drain()
                return None

            if broker_type not in feed_manager.get_brokers(symbol, market_type):
                writer.write(
                    self._err(
                        f"Broker '{broker_type}' for market type '{market_type}' "
                        f"for symbol '{symbol}' at index {idx} is not supported"
                    )
                )
                await writer.drain()
                return None

            for tf_raw in raw_timeframes:
                try:
                    timeframe = Timeframe(tf_raw)
                except ValueError:
                    writer.write(
                        self._err(
                            f"Invalid timeframe '{tf_raw}' at index {idx}"
                        )
                    )
                    await writer.drain()
                    return None

                if timeframe not in feed_manager.get_timeframes(
                    symbol, market_type, broker_type
                ):
                    writer.write(
                        self._err(
                            f"Timeframe '{timeframe}' for broker '{broker_type}' "
                            f"for market type '{market_type}' "
                            f"for symbol '{symbol}' at index {idx} is not supported"
                        )
                    )
                    await writer.drain()
                    return None

                expanded.append((symbol, market_type, broker_type, timeframe))

        conns = []
        for symbol, market_type, broker_type, timeframe in expanded:
            conn = SocketConnection(
                writer=writer,
                symbol=symbol,
                market_type=market_type,
                broker_type=broker_type,
                timeframe=timeframe,
            )
            self._register_live(conn)
            conns.append(conn)

        return conns

    def _register_live(self, conn: SocketConnection) -> None:
        self._live_conns[conn.symbol][conn.market_type][conn.broker_type][
            conn.timeframe
        ].add(conn)
        self._logger.info(
            "Connection %s bootstrapped into live stream (%s / %s / %s / %s)",
            conn.addr,
            conn.symbol,
            conn.market_type,
            conn.broker_type,
            conn.timeframe,
        )

    def _err(self, message: str) -> bytes:
        # return (json.dumps({"type": "error", "message": message}) + "\n").encode()
        return self._create_message({"type": "error", "message": message})

    def _ohlcmodel_payload(self, candle: OHLCSchema, is_live: bool = True) -> bytes:
        """Serialise a live OHLCSchema (from a feed) to a wire frame."""
        # return (
        #     json.dumps(
        #         {
        #             "candle": candle.model_dump(mode="json"),
        #             "is_live": is_live,
        #         }
        #     )
        #     + "\n"
        # ).encode()
        return self._create_message(
            {"candle": candle.model_dump(mode="json"), "is_live": is_live}
        )

    def _heartbeat_ack(self) -> bytes:
        # return (json.dumps({"type": "heartbeat_ack"}) + "\n").encode()
        return self._create_message({"type": "heartbeat_ack"})

    def _create_message(self, payload: dict):
        return (json.dumps(payload) + "\n").encode()
