from __future__ import annotations

import json
import logging
import socket
import threading
import time
from datetime import UTC, datetime
from typing import Generator

from module.broker.enums import BrokerType
from ..enums import MarketType, Timeframe
from ..schema import OHLC

logger = logging.getLogger(__name__)


class OHLCFeedSocketException(Exception):
    pass


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

        self._socket: socket.socket | None = None
        self._reader = None

        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_running = False

        self._logger = logging.getLogger(self.__class__.__name__)

        self._subscribe_payload: dict | None = None
        self._in_replay = False

    def connect(self) -> None:
        self._socket = socket.create_connection((self._host, self._port))

        # buffered reader for readline()
        self._reader = self._socket.makefile("rb")

        self._start_heartbeat()

        self._logger.info(
            "Connected to %s:%s",
            self._host,
            self._port,
        )

    def close(self) -> None:
        self._stop_heartbeat()

        try:
            if self._reader:
                self._reader.close()
        except Exception:
            pass

        try:
            if self._socket:
                self._socket.close()
        except Exception:
            pass

        self._reader = None
        self._socket = None

        self._logger.info("Connection closed")

    @property
    def is_connected(self) -> bool:
        return self._socket is not None

    def subscribe(
        self,
        symbol: str,
        market_type: MarketType,
        broker: BrokerType,
        timeframe: Timeframe,
        start: int | None = None,
    ) -> None:
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

        self._send(payload)

        self._logger.info(
            "Subscribed: %s / %s / %s / %s start=%s",
            symbol,
            market_type,
            broker,
            timeframe,
            start,
        )

    def candles(self) -> Generator[OHLC, None, None]:
        if self._subscribe_payload is None:
            raise RuntimeError("Call subscribe() before iterating candles()")

        attempts = 0

        while True:
            try:
                yield from self._read_loop()
                break

            except (
                ConnectionResetError,
                BrokenPipeError,
                EOFError,
                OSError,
            ) as exc:
                self._logger.warning("Connection lost: %s", exc)

            except OHLCFeedSocketException:
                raise

            except Exception as exc:
                self._logger.exception("Unexpected error: %s", exc)

            if not self._reconnect:
                break


            if (
                self._reconnect_attempts
                and attempts >= self._reconnect_attempts
            ):
                self._logger.error(
                    "Exhausted reconnection attempts"
                )
                break
            
            attempts += 1

            self._logger.info(
                "Reconnecting in %.1fs (attempt %d)...",
                self._reconnect_delay,
                attempts,
            )

            time.sleep(self._reconnect_delay)

            try:
                self.close()
                self.connect()

                if self._subscribe_payload:
                    self._send(self._subscribe_payload)

            except Exception as exc:
                self._logger.error(
                    "Reconnection failed: %s",
                    exc,
                )

    def _read_loop(self) -> Generator[OHLC, None, None]:

        while True:
            raw = self._readline()

            if not raw:
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

            if isinstance(frame, dict):

                if frame.get("type") == "heartbeat_ack":
                    continue

                if frame.get("type") == "error":
                    raise OHLCFeedSocketException(
                        frame.get("message", "unknown server error")
                    )

            candles = frame if isinstance(frame, list) else [frame]

            for raw_candle in candles:

                candle = self._parse_candle(raw_candle)

                if candle is None:
                    continue

                if self._in_replay:
                    self._send({"type": "replay_ack"})

                yield candle

    def _readline(self) -> bytes:
        if self._reader is None:
            raise ConnectionResetError("Not connected")

        return self._reader.readline()

    def _send(self, payload: dict) -> None:

        if self._socket is None:
            raise ConnectionResetError("Not connected")

        data = (json.dumps(payload) + "\n").encode()

        self._socket.sendall(data)

    def _start_heartbeat(self) -> None:

        self._heartbeat_running = True

        def loop() -> None:

            while self._heartbeat_running:

                time.sleep(self._heartbeat_interval)

                if not self.is_connected:
                    return

                try:
                    self._send({"type": "heartbeat"})

                except Exception as exc:
                    self._logger.warning(
                        "Heartbeat failed: %s",
                        exc,
                    )
                    return

        self._heartbeat_thread = threading.Thread(
            target=loop,
            daemon=True,
        )

        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:

        self._heartbeat_running = False

        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=1)

        self._heartbeat_thread = None

    def _parse_candle(self, raw: dict) -> OHLC | None:

        try:
            self._in_replay = not raw["is_live"]

            candle_data = raw["candle"]

            ts = candle_data["timestamp"]

            if isinstance(ts, (int, float)):
                timestamp = datetime.fromtimestamp(ts, tz=UTC)
            else:
                timestamp = datetime.fromisoformat(ts)

            return OHLC(
                open=float(candle_data["open"]),
                high=float(candle_data["high"]),
                low=float(candle_data["low"]),
                close=float(candle_data["close"]),
                volume=float(candle_data.get("volume", 0.0)),
                # timestamp=timestamp,
                timestamp=ts,
                timeframe=Timeframe(candle_data["timeframe"]),
                symbol=candle_data["symbol"],
                broker=BrokerType(candle_data["broker"]),
                market_type=MarketType(candle_data["market_type"]),
            )

        except Exception as exc:
            logger.warning(
                "Could not parse candle frame: %s — %r",
                exc,
                raw,
            )
            return None