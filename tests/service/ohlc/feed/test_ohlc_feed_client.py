import json
import logging
import socket
import threading
import time
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch, PropertyMock, call

import pytest

from enums import BrokerType, MarketType, Timeframe
from models import OHLC as OHLCModel
from service.ohlc.feed.client import (
    OHLCFeedClient,
    OHLCFeedSocketError,
)


def make_server_frame(**kwargs) -> dict:
    """Build a server wire frame (dict before JSON encoding)."""
    defaults = {
        "candle": {
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.0,
            "volume": 1000.0,
            "timestamp": 1500000000,
            "timeframe": "1m",
            "symbol": "AAPL",
            "broker": "alpaca",
            "market_type": "stocks",
        },
        "is_live": True,
    }
    if "candle" in kwargs:
        defaults["candle"].update(kwargs.pop("candle"))
    defaults.update(kwargs)
    return defaults


def make_ohlc_model(**kwargs) -> OHLCModel:
    defaults = {
        "open": 100.0,
        "high": 105.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 1000.0,
        "timestamp": 1500000000,
        "timeframe": Timeframe.m1,
        "symbol": "AAPL",
        "broker": BrokerType.ALPACA,
        "market_type": MarketType.STOCKS,
    }
    defaults.update(kwargs)
    return OHLCModel(**defaults)


@pytest.fixture
def client():
    return OHLCFeedClient(host="127.0.0.1", port=9000)


@pytest.fixture
def client_no_reconnect():
    return OHLCFeedClient(
        host="127.0.0.1",
        port=9000,
        reconnect=False,
    )


@pytest.fixture
def client_limited_reconnect():
    return OHLCFeedClient(
        host="127.0.0.1",
        port=9000,
        reconnect=True,
        reconnect_attempts=3,
        reconnect_delay=0.1,
    )


@pytest.fixture
def mock_socket():
    sock = MagicMock(spec=socket.socket)
    sock.makefile = MagicMock(return_value=MagicMock())
    sock.sendall = MagicMock()
    sock.close = MagicMock()
    return sock


class TestConnect:
    """Unit tests for connect method."""

    def test_connect_creates_socket(self, client):
        with patch("socket.create_connection") as mock_create:
            mock_sock = MagicMock(spec=socket.socket)
            mock_reader = MagicMock()
            mock_sock.makefile = MagicMock(return_value=mock_reader)
            mock_create.return_value = mock_sock

            client.connect()

            mock_create.assert_called_once_with(("127.0.0.1", 9000))
            assert client._socket == mock_sock
            assert client._reader == mock_reader

    def test_connect_starts_heartbeat(self, client):
        with patch("socket.create_connection") as mock_create:
            mock_sock = MagicMock(spec=socket.socket)
            mock_sock.makefile = MagicMock(return_value=MagicMock())
            mock_create.return_value = mock_sock

            with patch.object(client, "_start_heartbeat") as mock_start_hb:
                client.connect()
                mock_start_hb.assert_called_once()

    def test_connect_logs(self, client, caplog):
        with patch("socket.create_connection") as mock_create:
            mock_sock = MagicMock(spec=socket.socket)
            mock_sock.makefile = MagicMock(return_value=MagicMock())
            mock_create.return_value = mock_sock

            with caplog.at_level(logging.INFO):
                client.connect()

            assert "Connected to 127.0.0.1:9000" in caplog.text


class TestClose:
    """Unit tests for close method."""

    def test_close_stops_heartbeat(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._reader = MagicMock()
        client._heartbeat_thread = MagicMock()

        with patch.object(client, "_stop_heartbeat") as mock_stop_hb:
            client.close()
            mock_stop_hb.assert_called_once()

    def test_close_closes_reader(self, client):
        mock_reader = MagicMock()
        client._reader = mock_reader
        client._socket = MagicMock(spec=socket.socket)

        client.close()

        mock_reader.close.assert_called_once()

    def test_close_closes_socket(self, client):
        mock_sock = MagicMock(spec=socket.socket)
        client._socket = mock_sock
        client._reader = MagicMock()

        client.close()

        mock_sock.close.assert_called_once()

    def test_close_nullifies_refs(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._reader = MagicMock()
        client._heartbeat_thread = MagicMock()

        client.close()

        assert client._socket is None
        assert client._reader is None
        assert client._heartbeat_thread is None

    def test_close_swallows_reader_exception(self, client):
        mock_reader = MagicMock()
        mock_reader.close = MagicMock(side_effect=OSError("already closed"))
        client._reader = mock_reader
        client._socket = MagicMock(spec=socket.socket)

        # Should not raise
        client.close()

    def test_close_swallows_socket_exception(self, client):
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.close = MagicMock(side_effect=OSError("already closed"))
        client._socket = mock_sock
        client._reader = MagicMock()

        # Should not raise
        client.close()

    def test_close_logs(self, client, caplog):
        client._socket = MagicMock(spec=socket.socket)
        client._reader = MagicMock()

        with caplog.at_level(logging.INFO):
            client.close()

        assert "Connection closed" in caplog.text


class TestSubscribe:
    """Unit tests for subscribe method."""

    def test_subscribe_builds_payload(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        client.subscribe(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
            start=1500000000,
        )

        assert client._subscribe_payload == {
            "type": "subscribe",
            "symbol": "AAPL",
            "market_type": "stocks",
            "broker": "alpaca",
            "timeframe": "1m",
            "start": 1500000000,
        }

    def test_subscribe_sets_in_replay_when_start_provided(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        client.subscribe(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
            start=1500000000,
        )

        assert client._in_replay is True

    def test_subscribe_no_replay_when_start_none(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        client.subscribe(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
            start=None,
        )

        assert client._in_replay is False

    def test_subscribe_sends_payload(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        client.subscribe(
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )

        client._socket.sendall.assert_called_once()
        sent = client._socket.sendall.call_args.args[0]
        data = json.loads(sent.decode().strip())
        assert data["type"] == "subscribe"
        assert data["symbol"] == "AAPL"

    def test_subscribe_logs(self, client, caplog):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        with caplog.at_level(logging.INFO):
            client.subscribe(
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                start=1500000000,
            )

        assert "Subscribed: AAPL" in caplog.text
        assert "start=1500000000" in caplog.text


class TestSend:
    """Unit tests for _send method."""

    def test_send_encodes_json_with_newline(self, client):
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()

        client._send({"type": "heartbeat"})

        client._socket.sendall.assert_called_once()
        sent = client._socket.sendall.call_args.args[0]
        assert sent == b'{"type": "heartbeat"}\n'

    def test_send_raises_when_not_connected(self, client):
        client._socket = None

        with pytest.raises(ConnectionResetError, match="Not connected"):
            client._send({"type": "heartbeat"})


class TestReadline:
    """Unit tests for _readline method."""

    def test_readline_returns_bytes(self, client):
        client._reader = MagicMock()
        client._reader.readline = MagicMock(return_value=b'{"type":"heartbeat_ack"}\n')

        result = client._readline()

        assert result == b'{"type":"heartbeat_ack"}\n'

    def test_readline_raises_when_not_connected(self, client):
        client._reader = None

        with pytest.raises(ConnectionResetError, match="Not connected"):
            client._readline()


class TestParseCandle:
    """Unit tests for _parse_candle method."""

    def test_parse_candle_success(self, client):
        frame = make_server_frame()
        result = client._parse_candle(frame)

        assert isinstance(result, OHLCModel), result
        assert result.open == 100.0
        assert result.high == 105.0
        assert result.low == 99.0
        assert result.close == 102.0
        assert result.volume == 1000.0
        assert result.timestamp == 1500000000
        assert result.timeframe == Timeframe.m1
        assert result.symbol == "AAPL"
        assert result.broker == BrokerType.ALPACA
        assert result.market_type == MarketType.STOCKS
        assert client._in_replay is False  # is_live=True -> not replay

    def test_parse_candle_replay_sets_in_replay(self, client):
        frame = make_server_frame(is_live=False)
        result = client._parse_candle(frame)

        assert result is not None
        assert client._in_replay is True

    def test_parse_candle_missing_volume_defaults_to_zero(self, client):
        frame = make_server_frame()
        frame["candle"].pop("volume")
        result = client._parse_candle(frame)

        assert result.volume == 0.0

    def test_parse_candle_different_timeframe(self, client):
        frame = make_server_frame(
            candle={"timeframe": "1h"},
        )
        result = client._parse_candle(frame)

        assert result.timeframe == Timeframe.H1

    def test_parse_candle_different_broker(self, client):
        frame = make_server_frame(
            candle={"broker": "alpaca"},
        )
        result = client._parse_candle(frame)

        assert result.broker == BrokerType.ALPACA


class TestReadLoop:
    """Unit tests for _read_loop generator."""

    def test_read_loop_yields_candle(self, client):
        frame = make_server_frame()
        client._reader = MagicMock()
        client._reader.readline = MagicMock(
            return_value=(json.dumps(frame) + "\n").encode()
        )

        # Second call returns empty to end generator
        client._reader.readline.side_effect = [
            (json.dumps(frame) + "\n").encode(),
            b"",
        ]

        candles = list(client._read_loop())

        assert len(candles) == 1
        assert isinstance(candles[0], OHLCModel)
        assert candles[0].symbol == "AAPL"

    def test_read_loop_heartbeat_ack_skipped(self, client):
        client._reader = MagicMock()
        client._reader.readline.side_effect = [
            (json.dumps({"type": "heartbeat_ack"}) + "\n").encode(),
            b"",
        ]

        candles = list(client._read_loop())

        assert candles == []

    def test_read_loop_error_raises(self, client):
        client._reader = MagicMock()
        client._reader.readline.side_effect = [
            (json.dumps({"type": "error", "message": "bad request"}) + "\n").encode(),
        ]

        with pytest.raises(OHLCFeedSocketError, match="bad request"):
            list(client._read_loop())

    def test_read_loop_error_default_message(self, client):
        client._reader = MagicMock()
        client._reader.readline.side_effect = [
            (json.dumps({"type": "error"}) + "\n").encode(),
        ]

        with pytest.raises(OHLCFeedSocketError, match="unknown server error"):
            list(client._read_loop())

    def test_read_loop_replay_sends_ack(self, client):
        frame = make_server_frame(is_live=False)
        client._reader = MagicMock()
        client._reader.readline.side_effect = [
            (json.dumps(frame) + "\n").encode(),
            b"",
        ]
        client._socket = MagicMock(spec=socket.socket)
        client._socket.sendall = MagicMock()
        client._in_replay = True

        list(client._read_loop())

        client._socket.sendall.assert_called_once()
        sent = client._socket.sendall.call_args.args[0]
        data = json.loads(sent.decode().strip())
        assert data["type"] == "replay_ack"

    def test_read_loop_eof_returns(self, client):
        client._reader = MagicMock()
        client._reader.readline = MagicMock(return_value=b"")

        candles = list(client._read_loop())

        assert candles == []

    def test_read_loop_list_frame(self, client):
        """Server may send a list of candles."""
        frame1 = make_server_frame(candle={"timestamp": 1500000000})
        frame2 = make_server_frame(candle={"timestamp": 1500000060})
        client._reader = MagicMock()
        client._reader.readline.side_effect = [
            (json.dumps([frame1, frame2]) + "\n").encode(),
            b"",
        ]

        candles = list(client._read_loop())

        assert len(candles) == 2
        assert candles[0].timestamp == 1500000000
        assert candles[1].timestamp == 1500000060


class TestStartHeartbeat:
    """Unit tests for _start_heartbeat."""

    def test_start_heartbeat_sets_flag(self, client):
        with patch("threading.Thread"):
            client._start_heartbeat()

        assert client._heartbeat_running is True

    def test_start_heartbeat_creates_thread(self, client):
        with patch("threading.Thread") as mock_thread:
            mock_thread.return_value = MagicMock()
            client._start_heartbeat()

            mock_thread.assert_called_once()
            kwargs = mock_thread.call_args.kwargs
            assert kwargs["daemon"] is True

    def test_start_heartbeat_starts_thread(self, client):
        mock_thread = MagicMock()
        with patch("threading.Thread", return_value=mock_thread):
            client._start_heartbeat()

        mock_thread.start.assert_called_once()


class TestStopHeartbeat:
    """Unit tests for _stop_heartbeat."""

    def test_stop_heartbeat_clears_flag(self, client):
        client._heartbeat_running = True
        client._heartbeat_thread = MagicMock()

        client._stop_heartbeat()

        assert client._heartbeat_running is False

    def test_stop_heartbeat_joins_thread(self, client):
        mock_thread = MagicMock()
        client._heartbeat_thread = mock_thread
        client._heartbeat_running = True

        client._stop_heartbeat()

        mock_thread.join.assert_called_once_with(timeout=1)

    def test_stop_heartbeat_nullifies_thread(self, client):
        client._heartbeat_thread = MagicMock()
        client._heartbeat_running = True

        client._stop_heartbeat()

        assert client._heartbeat_thread is None

    def test_stop_heartbeat_no_thread(self, client):
        client._heartbeat_thread = None
        client._heartbeat_running = True

        # Should not raise
        client._stop_heartbeat()

        assert client._heartbeat_running is False


class TestCandles:
    """Unit tests for candles() generator with reconnection logic."""

    def test_candles_raises_without_subscribe(self, client):
        with pytest.raises(RuntimeError, match="subscribe\\(\\) before iterating"):
            list(client.candles())

    def test_candles_yields_from_read_loop(self, client):
        client._subscribe_payload = {"type": "subscribe"}
        frame = make_server_frame()

        with patch.object(client, "_read_loop") as mock_read_loop:
            mock_read_loop.return_value = iter([make_ohlc_model()])
            candles = list(client.candles())

        assert len(candles) == 1
        assert candles[0].symbol == "AAPL"

    def test_candles_no_reconnect_on_error(self, client_no_reconnect):
        client = client_no_reconnect
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            mock_read_loop.side_effect = ConnectionResetError("broken")

            candles = list(client.candles())

        assert candles == []
        mock_read_loop.assert_called_once()

    def test_candles_reconnects_on_connection_reset(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close") as mock_close:
                with patch.object(client, "connect") as mock_connect:
                    # First call fails, second succeeds
                    mock_read_loop.side_effect = [
                        ConnectionResetError("broken"),
                        [make_ohlc_model()],
                    ]

                    candles = list(client.candles())

        assert len(candles) == 1
        mock_close.assert_called_once()
        mock_connect.assert_called_once()
        # Resubscribe after reconnect
        assert client._subscribe_payload is not None

    def test_candles_reconnects_on_broken_pipe(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close"):
                with patch.object(client, "connect"):
                    mock_read_loop.side_effect = [
                        BrokenPipeError(),
                        [make_ohlc_model()],
                    ]

                    candles = list(client.candles())

        assert len(candles) == 1

    def test_candles_reconnects_on_eof(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close"):
                with patch.object(client, "connect"):
                    mock_read_loop.side_effect = [
                        EOFError(),
                        [make_ohlc_model()],
                    ]

                    candles = list(client.candles())

        assert len(candles) == 1

    def test_candles_reconnects_on_oserror(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close"):
                with patch.object(client, "connect"):
                    mock_read_loop.side_effect = [
                        OSError("network down"),
                        [make_ohlc_model()],
                    ]

                    candles = list(client.candles())

        assert len(candles) == 1

    def test_candles_raises_on_ohlc_socket_error(self, client):
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            mock_read_loop.side_effect = OHLCFeedSocketError("server error")

            with pytest.raises(OHLCFeedSocketError, match="server error"):
                list(client.candles())

    def test_candles_exhausts_attempts(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._reconnect_attempts = 2
        client._subscribe_payload = {"type": "subscribe"}

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close"):
                with patch.object(client, "connect"):
                    # Always fails
                    mock_read_loop.side_effect = ConnectionResetError("broken")

                    candles = list(client.candles())

        assert candles == []
        # Initial attempt + 2 reconnects = 3 calls
        assert mock_read_loop.call_count == 3

    def test_candles_reconnect_resubscribes(self, client_limited_reconnect):
        client = client_limited_reconnect
        client._subscribe_payload = {
            "type": "subscribe",
            "symbol": "AAPL",
        }

        with patch.object(client, "_read_loop") as mock_read_loop:
            with patch.object(client, "close"):
                with patch.object(client, "connect"):
                    with patch.object(client, "_send") as mock_send:
                        mock_read_loop.side_effect = [
                            ConnectionResetError("broken"),
                            [make_ohlc_model()],
                        ]

                        list(client.candles())

        # Should resend subscribe payload after reconnect
        mock_send.assert_called_once_with(client._subscribe_payload)


class TestIntegration:
    """Integration-style tests for OHLCFeedClient."""

    def test_full_connect_subscribe_candle_flow(self, client):
        """Test connect -> subscribe -> read candle -> close."""
        frame = make_server_frame()

        with patch("socket.create_connection") as mock_create:
            mock_sock = MagicMock(spec=socket.socket)
            mock_reader = MagicMock()
            mock_reader.readline = MagicMock(
                return_value=(json.dumps(frame) + "\\n").encode()
            )
            mock_sock.makefile = MagicMock(return_value=mock_reader)
            mock_create.return_value = mock_sock

            client.connect()
            client.subscribe(
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
            )

            # Simulate EOF after one candle
            mock_reader.readline.side_effect = [
                (json.dumps(frame) + "\\n").encode(),
                b"",
            ]

            candles = list(client.candles())

            assert len(candles) == 1
            assert candles[0].open == 100.0
            assert candles[0].symbol == "AAPL"

            client.close()
            assert client.is_connected is False

    def test_replay_flow(self, client):
        """Test subscribe with start -> replay_ack -> live transition."""
        replay_frame = make_server_frame(is_live=False)
        live_frame = make_server_frame(is_live=True)

        with patch("socket.create_connection") as mock_create:
            mock_sock = MagicMock(spec=socket.socket)
            mock_reader = MagicMock()
            mock_sock.makefile = MagicMock(return_value=mock_reader)
            mock_create.return_value = mock_sock

            client.connect()
            client.subscribe(
                symbol="AAPL",
                market_type=MarketType.STOCKS,
                broker=BrokerType.ALPACA,
                timeframe=Timeframe.m1,
                start=1500000000,
            )

            # First replay candle, then live candle, then EOF
            mock_reader.readline.side_effect = [
                (json.dumps(replay_frame) + "\n").encode(),
                (json.dumps(live_frame) + "\n").encode(),
                b"",
            ]

            candles = list(client.candles())

            assert len(candles) == 2
            # First is replay
            assert client._in_replay is False  # After parsing live frame
            # replay_ack should have been sent after first candle
            calls = mock_sock.sendall.call_args_list
            # First call is subscribe, second should be replay_ack
            assert len(calls) >= 2
            ack_data = json.loads(calls[1].args[0].decode().strip())
            assert ack_data["type"] == "replay_ack"
