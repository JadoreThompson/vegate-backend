from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, UTC
from typing import AsyncGenerator

from enums import BrokerType, MarketType, Timeframe
from models import OHLC as OHLCModel

logger = logging.getLogger(__name__)


class OHLCFeedSocketError(Exception):
    """Raised when the server sends an error frame."""


class OHLCFeedClient:

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        *,
        reconnect: bool = True,
        reconnect_delay: float = 2.0,
        reconnect_attempts: int = 0,
    ) -> None:
        self._host = host
        self._port = port
        self._reconnect = reconnect
        self._reconnect_delay = reconnect_delay
        self._reconnect_attempts = reconnect_attempts

        self._heartbeat_interval = 15.0
        self._heartbeat_task: asyncio.Task | None = None
        self._last_read_ts: float = asyncio.get_event_loop().time()

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._subscribe_payload: dict | None = None
        self._in_replay: bool = False

    async def __aenter__(self) -> OHLCFeedClient:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def connect(self) -> None:
        """Open the TCP connection to the server."""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
        )

        self._last_read_ts = asyncio.get_event_loop().time()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        self._logger.info("Connected to %s:%s", self._host, self._port)

    async def close(self) -> None:
        """Close the connection gracefully."""

        if self._heartbeat_task:
            self._heartbeat_task.cancel()

            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

            self._heartbeat_task = None

        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

            self._writer = None
            self._reader = None

        self._logger.info("Connection closed")

        # Add heartbeat loop

    async def _heartbeat_loop(self) -> None:
        """
        Periodically send heartbeat pings to keep the
        connection alive and detect dead sockets.
        """

        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)

                if not self.is_connected:
                    return

                try:
                    await self._send({"type": "heartbeat"})

                except Exception as exc:
                    self._logger.warning(
                        "Heartbeat failed: %s",
                        exc,
                    )
                    return

        except asyncio.CancelledError:
            return

    @property
    def is_connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def subscribe(
        self,
        symbol: str,
        market_type: MarketType,
        broker: BrokerType,
        timeframe: Timeframe,
        start: int | None = None,
    ) -> None:
        """
        Send a subscribe message to the server.

        Args:
            symbol: Instrument symbol, e.g. ``"BTC/USD"``.
            market_type: ``MarketType`` enum value.
            broker: ``BrokerType`` enum value.
            timeframe: ``Timeframe`` enum value.
            start: Optional Unix timestamp. When provided, the server replays
                all stored candles from that point before switching to live data.
        """
        payload = {
            "type": "subscribe",
            "symbol": symbol,
            "market_type": market_type.value,
            "broker": broker.value,
            "timeframe": timeframe.value,
            "start": start,
        }

        self._subscribe_payload = payload
        self._in_replay = start is not None

        await self._send(payload)
        self._logger.info(
            "Subscribed: %s / %s / %s / %s  start=%s",
            symbol,
            market_type,
            broker,
            timeframe,
            start,
        )

    async def candles(self) -> AsyncGenerator[OHLCModel, None]:
        """
        Yield candles as they arrive from the server.

        Replay acknowledgements are sent automatically. The caller does not
        need to know whether a frame is historical or live.

        Reconnection (if enabled) is handled transparently. The generator
        re-subscribes and continues yielding without raising.

        Raises:
            MarketSocketError: If the server sends an error frame and
                ``reconnect`` is False, or if all reconnection attempts
                are exhausted.
            RuntimeError: If ``subscribe()`` has not been called before
                iterating.
        """
        if self._subscribe_payload is None:
            raise RuntimeError("Call subscribe() before iterating candles()")

        attempts = 0

        while True:
            try:
                async for candle in self._read_loop():
                    yield candle
                # Server closed the connection cleanly
                break

            except (ConnectionResetError, BrokenPipeError, EOFError) as exc:
                self._logger.warning("Connection lost: %s", exc)

            except OHLCFeedSocketError:
                raise

            except Exception as exc:
                self._logger.exception("Unexpected error: %s", exc)

            if not self._reconnect:
                break

            attempts += 1
            if self._reconnect_attempts and attempts >= self._reconnect_attempts:
                self._logger.error(
                    "Exhausted %d reconnection attempt(s)", self._reconnect_attempts
                )
                break

            self._logger.info(
                "Reconnecting in %.1fs (attempt %d)…",
                self._reconnect_delay,
                attempts,
            )
            await asyncio.sleep(self._reconnect_delay)

            try:
                await self.close()
                await self.connect()
                await self._send(self._subscribe_payload)
            except Exception as exc:
                self._logger.error("Reconnection failed: %s", exc)
                continue

    async def _read_loop(self) -> AsyncGenerator[OHLCModel, None]:
        """
        Core read loop: read frames, ACK replays, yield parsed candles.
        Exits when the server closes the connection (EOF).
        """

        while True:
            raw = await self._readline()

            self._last_read_ts = asyncio.get_event_loop().time()

            if not raw:
                # EOF
                return

            try:
                frame = json.loads(raw.decode())

            except json.JSONDecodeError as exc:
                self._logger.error(
                    "Could not parse frame: %s — %r",
                    exc,
                    raw[:200],
                )
                continue

            # Heartbeat ACK
            if isinstance(frame, dict) and frame.get("type") == "heartbeat_ack":
                continue

            # Error frame from server
            if isinstance(frame, dict) and frame.get("type") == "error":
                raise OHLCFeedSocketError(frame.get("message", "unknown server error"))

            # Candle frame - the server sends a JSON array with one object
            candles = frame if isinstance(frame, list) else [frame]

            for raw_candle in candles:
                candle = self._parse_candle(raw_candle)

                if candle is None:
                    continue

                # If we are in replay mode, ACK so the server sends the next frame
                if self._in_replay:
                    await self._send({"type": "replay_ack"})

                yield candle

    async def _readline(self) -> bytes:
        """Read one newline-terminated frame, respecting the read timeout."""
        coro = self._reader.readline()
        return await coro

    async def _send(self, payload: dict) -> None:
        """Serialise *payload* and write it to the socket."""
        if self._writer is None or self._writer.is_closing():
            raise ConnectionResetError("Not connected")
        self._writer.write((json.dumps(payload) + "\n").encode())
        await self._writer.drain()

    def _parse_candle(self, raw: dict) -> OHLCModel | None:
        """
        Convert a raw candle dict (as sent by the server) into an OHLCModel.
        Returns None and logs a warning if the frame cannot be parsed.
        """
        try:
            self._in_replay = not raw["is_live"]

            candle_data: dict = raw["candle"]
            ts = candle_data["timestamp"]
            if isinstance(ts, (int, float)):
                timestamp = datetime.fromtimestamp(ts, tz=UTC)
            else:
                timestamp = datetime.fromisoformat(ts)

            return OHLCModel(
                open=float(candle_data["open"]),
                high=float(candle_data["high"]),
                low=float(candle_data["low"]),
                close=float(candle_data["close"]),
                volume=float(candle_data.get("volume", 0.0)),
                timestamp=timestamp,
                timeframe=Timeframe(candle_data["timeframe"]),
                symbol=candle_data["symbol"],
                broker=BrokerType(candle_data["broker"]),
                market_type=MarketType(candle_data["market_type"]),
            )
        except Exception as exc:
            logger.warning("Could not parse candle frame: %s — %r", exc, raw)
            return None
