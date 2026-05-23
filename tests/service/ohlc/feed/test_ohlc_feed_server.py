import asyncio
import json
import pytest
import pytest_asyncio
from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock, call
from uuid import uuid4

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.model import OHLC
from module.markets.schema import OHLC as OHLCModel
from module.markets.feed.base import OHLCFeed
from module.markets.feed.manager import feed_manager
from module.markets.feed.server import (
    OHLCFeedServer,
    SocketConnection,
)


def make_ohlc_row(**kwargs):
    """Build a MagicMock that looks like an OHLC DB row."""
    defaults = {
        "open": 100.0,
        "high": 105.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 1000.0,
        "timestamp": 1500000000,
        "timeframe": Timeframe.m1,
        "symbol": "AAPL",
        "source": BrokerType.ALPACA,
        "market_type": MarketType.STOCKS,
    }
    defaults.update(kwargs)
    row = MagicMock(spec=OHLC)
    for k, v in defaults.items():
        setattr(row, k, v)
    return row


def make_ohlc_model(**kwargs):
    """Build an OHLCModel instance."""
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
def server():
    return OHLCFeedServer(host="127.0.0.1", port=9000)


@pytest.fixture
def mock_writer():
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.get_extra_info = MagicMock(return_value=("192.168.1.1", 54321))
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    return writer


@pytest.fixture
def socket_conn(mock_writer):
    return SocketConnection(
        writer=mock_writer,
        symbol="AAPL",
        market_type=MarketType.STOCKS,
        broker=BrokerType.ALPACA,
        timeframe=Timeframe.m1,
    )


@pytest.fixture
def mock_feed():
    feed = MagicMock(spec=OHLCFeed)
    feed.symbol = "AAPL"
    feed.market_type = MarketType.STOCKS
    feed.broker = BrokerType.ALPACA
    feed.timeframe = Timeframe.m1
    feed.name = "MockFeed-AAPL-m1"
    feed.stop = AsyncMock()
    feed.set_on_candle = MagicMock()
    return feed


@pytest.fixture
def mock_feed_crypto():
    feed = MagicMock(spec=OHLCFeed)
    feed.symbol = "BTC/USD"
    feed.market_type = MarketType.CRYPTO
    feed.broker = BrokerType.ALPACA
    feed.timeframe = Timeframe.H1
    feed.name = "MockFeed-BTC-H1"
    feed.stop = AsyncMock()
    feed.set_on_candle = MagicMock()
    return feed


class TestErr:
    """Unit tests for _err helper."""

    def test_err_returns_bytes(self, server):
        result = server._err("Something went wrong")

        assert isinstance(result, bytes)
        assert result.endswith(b"\n"), result

        data = json.loads(result.decode().strip())

        assert data["type"] == "error", data
        assert data["message"] == "Something went wrong", data


class TestCandlePayload:
    """Unit tests for _candle_payload helper."""

    def test_candle_payload_has_candle_data(self, server):
        row = make_ohlc_row(open=150.0, close=155.0)
        result = server._candle_payload(row)

        assert isinstance(result, bytes), result
        assert result.endswith(b"\n"), result

        data = json.loads(result.decode().strip())

        assert data["candle"]["open"] == 150.0, data
        assert data["candle"]["close"] == 155.0, data
        assert data["candle"]["symbol"] == "AAPL", data
        assert data["is_live"] is False, data


class TestOhlcModelPayload:
    """Unit tests for _ohlcmodel_payload helper."""

    def test_ohlcmodel_payload_has_candle_data_live(self, server):
        candle = make_ohlc_model(open=200.0, high=210.0, symbol="MSFT")
        result = server._ohlcmodel_payload(candle)

        assert isinstance(result, bytes), result
        assert result.endswith(b"\n"), result

        data = json.loads(result.decode().strip())

        assert data["candle"]["open"] == 200.0, data
        assert data["candle"]["high"] == 210.0, data
        assert data["candle"]["symbol"] == "MSFT", data
        assert data["is_live"] is True, data

    def test_ohlcmodel_payload_has_candle_data_not_live(self, server):
        candle = make_ohlc_model(open=200.0, high=210.0, symbol="MSFT")
        result = server._ohlcmodel_payload(candle, is_live=False)

        assert isinstance(result, bytes), result
        assert result.endswith(b"\n"), result

        data = json.loads(result.decode().strip())

        assert data["candle"]["open"] == 200.0, data
        assert data["candle"]["high"] == 210.0, data
        assert data["candle"]["symbol"] == "MSFT", data
        assert data["is_live"] is False, data


class TestHeartbeatAck:
    """Unit tests for _heartbeat_ack helper."""

    def test_heartbeat_ack_structure(self, server):
        result = server._heartbeat_ack()
        assert isinstance(result, bytes)
        assert result.endswith(b"\n"), result

        data = json.loads(result.decode().strip())
        assert data["type"] == "heartbeat_ack"


class TestSocketConnectionSendNowait:
    """Unit tests for SocketConnection.send_nowait."""

    def test_send_nowait_writes_data(self, socket_conn, mock_writer):
        data = b"hello\\n"
        socket_conn.send_nowait(data)
        mock_writer.write.assert_called_once_with(data)

    def test_send_nowait_does_not_drain(self, socket_conn, mock_writer):
        socket_conn.send_nowait(b"test")
        mock_writer.drain.assert_not_called()


class TestSocketConnectionSend:
    """Unit tests for SocketConnection.send (async)."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_send_writes_and_drains(self, socket_conn, mock_writer):
        data = b"hello\\n"
        await socket_conn.send(data)
        mock_writer.write.assert_called_once_with(data)
        mock_writer.drain.assert_awaited_once()


class TestSocketConnectionClose:
    """Unit tests for SocketConnection.close (async)."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_close_closes_writer(self, socket_conn, mock_writer):
        await socket_conn.close()
        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_close_swallows_exceptions(self, mock_writer):
        mock_writer.close = MagicMock(side_effect=OSError("already closed"))
        conn = SocketConnection(
            writer=mock_writer,
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )
        # Should not raise
        await conn.close()


class TestOHLCFeedServerStop:
    """Unit tests for OHLCFeedServer.stop."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_closes_server(self, server):
        mock_server = MagicMock()
        mock_server.close = MagicMock()
        mock_server.wait_closed = AsyncMock()
        server._server = mock_server

        with patch.object(feed_manager, "stop_all", AsyncMock()):
            await server.stop()

        mock_server.close.assert_called_once()
        mock_server.wait_closed.assert_awaited_once()


class TestOHLCFeedServerHandleCandle:
    """Unit tests for OHLCFeedServer.handle_candle (broadcast)."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_sends_to_live_connections(self, server, socket_conn):
        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].add(socket_conn)

        with patch.object(socket_conn, "send", AsyncMock()) as mock_send:
            await server.handle_candle(candle)
            mock_send.assert_awaited_once()

            # Verify payload is OHLCModel payload
            args = mock_send.await_args
            assert isinstance(args.args[0], bytes)

            data = json.loads(args.args[0].decode().strip())
            assert data["is_live"] is True
            assert data["candle"]["symbol"] == "AAPL"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_multiple_connections(
        self, server, socket_conn, mock_writer
    ):
        conn2 = SocketConnection(
            writer=mock_writer,
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )
        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].update([socket_conn, conn2])

        with patch.object(socket_conn, "send", AsyncMock()) as mock_send1:
            with patch.object(conn2, "send", AsyncMock()) as mock_send2:
                await server.handle_candle(candle)
                mock_send1.assert_awaited_once()
                mock_send2.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_drops_dead_connections(self, server, socket_conn):
        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].add(socket_conn)

        with patch.object(
            socket_conn, "send", AsyncMock(side_effect=ConnectionResetError())
        ):
            await server.handle_candle(candle)

        # Dead connection should be removed
        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert socket_conn not in conns

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_drops_broken_pipe(self, server, socket_conn):
        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].add(socket_conn)

        with patch.object(
            socket_conn, "send", AsyncMock(side_effect=BrokenPipeError())
        ):
            await server.handle_candle(candle)

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert socket_conn not in conns

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_drops_cancelled(self, server, socket_conn):
        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].add(socket_conn)

        with patch.object(
            socket_conn, "send", AsyncMock(side_effect=asyncio.CancelledError())
        ):
            await server.handle_candle(candle)

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert socket_conn not in conns

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_candle_logs_broadcast(self, server, socket_conn, caplog):
        import logging

        candle = make_ohlc_model()
        server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ].add(socket_conn)

        with caplog.at_level(logging.INFO):
            with patch.object(socket_conn, "send", AsyncMock()):
                await server.handle_candle(candle)

        assert "Broadcasting candle" in caplog.text


class TestOHLCFeedServerHandleClient:
    """Unit tests for OHLCFeedServer._handle_client."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_eof_closes(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(return_value=b"")

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        writer.close.assert_called_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_heartbeat(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(
            side_effect=[
                json.dumps({"type": "heartbeat"}).encode() + b"\n",
                b"",  # EOF
            ]
        )

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        # Should have written heartbeat_ack
        calls = writer.write.call_args_list
        assert any(
            json.dumps({"type": "heartbeat_ack"}).encode() in c.args[0] for c in calls
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_invalid_json(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(
            side_effect=[
                b"not json\n",
                b"",  # EOF
            ]
        )

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        calls = writer.write.call_args_list
        assert any(b"Invalid JSON" in c.args[0] for c in calls)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_unsupported_type(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(
            side_effect=[
                json.dumps({"type": "unknown"}).encode() + b"\n",
                b"",  # EOF
            ]
        )

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        calls = writer.write.call_args_list
        assert any(b"Unsupported message type" in c.args[0] for c in calls)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_timeout(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)

        async def _readline():
            await asyncio.sleep(60)

        reader.readline = _readline

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        writer.close.assert_called_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_connection_reset(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(side_effect=ConnectionResetError())

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        # Should not raise, connection handled gracefully
        writer.close.assert_called_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_replay_ack_without_subscribe(self, server):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = AsyncMock(
            side_effect=[
                json.dumps({"type": "replay_ack"}).encode() + b"\n",
                b"",  # EOF
            ]
        )

        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 12345))
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        await server._handle_client(reader, writer)

        calls = writer.write.call_args_list
        assert any(
            b"Must subscribe before sending replay_ack" in c.args[0] for c in calls
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_client_cleans_up_live_conn_on_exit(self, server):
        # First register a feed so subscribe succeeds
        mock_feed = MagicMock(spec=OHLCFeed)
        mock_feed.symbol = "AAPL"
        mock_feed.market_type = MarketType.STOCKS
        mock_feed.broker = BrokerType.ALPACA
        mock_feed.timeframe = Timeframe.m1
        mock_feed.name = "TestFeed"
        mock_feed.stop = AsyncMock()
        mock_feed.set_on_candle = MagicMock()

        with patch.object(feed_manager, "register", AsyncMock()):
            await server.init([mock_feed])

        # Mock feed_manager lookups
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        reader = MagicMock(spec=asyncio.StreamReader)
                        reader.readline = AsyncMock(
                            side_effect=[
                                json.dumps(
                                    {
                                        "type": "subscribe",
                                        "symbol": "AAPL",
                                        "market_type": "stocks",
                                        "broker": "alpaca",
                                        "timeframe": "1m",
                                    }
                                ).encode()
                                + b"\n",
                                b"",  # EOF
                            ]
                        )

                        writer = MagicMock(spec=asyncio.StreamWriter)
                        writer.get_extra_info = MagicMock(
                            return_value=("10.0.0.1", 12345)
                        )
                        writer.write = MagicMock()
                        writer.drain = AsyncMock()
                        writer.close = MagicMock()
                        writer.wait_closed = AsyncMock()

                        await server._handle_client(reader, writer)

                        # Verify connection was registered then cleaned up
                        live_set = server._live_conns["AAPL"][MarketType.STOCKS][
                            BrokerType.ALPACA
                        ][Timeframe.m1]
                        assert len(live_set) == 0


class TestOHLCFeedServerHandleSubscribe:
    """Unit tests for OHLCFeedServer._handle_subscribe."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_bad_payload_missing_key(self, server, mock_writer):
        payload = {
            "symbol": "AAPL",
            "market_type": "stocks",
        }  # missing broker, timeframe
        result = await server._handle_subscribe(payload, mock_writer)
        assert result is None
        assert any(
            b"Bad subscribe payload" in c.args[0]
            for c in mock_writer.write.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_bad_payload_invalid_enum(self, server, mock_writer):
        payload = {
            "symbol": "AAPL",
            "market_type": "invalid_market",
            "broker": "alpaca",
            "timeframe": "m1",
        }
        result = await server._handle_subscribe(payload, mock_writer)
        assert result is None
        assert any(
            b"Bad subscribe payload" in c.args[0]
            for c in mock_writer.write.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_symbol(self, server, mock_writer):
        with patch.object(feed_manager, "get_symbols", return_value=set()):
            payload = {
                "symbol": "AAPL",
                "market_type": "stocks",
                "broker": "alpaca",
                "timeframe": "1m",
            }
            result = await server._handle_subscribe(payload, mock_writer)
            assert result is None
            assert any(
                b"is not supported" in c.args[0]
                for c in mock_writer.write.call_args_list
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_market_type(self, server, mock_writer):
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(feed_manager, "get_market_types", return_value=set()):
                payload = {
                    "symbol": "AAPL",
                    "market_type": "stocks",
                    "broker": "alpaca",
                    "timeframe": "1m",
                }
                result = await server._handle_subscribe(payload, mock_writer)
                assert result is None
                assert any(
                    b"Market type" in c.args[0]
                    for c in mock_writer.write.call_args_list
                )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_broker(self, server, mock_writer):
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(feed_manager, "get_brokers", return_value=set()):
                    payload = {
                        "symbol": "AAPL",
                        "market_type": "stocks",
                        "broker": "alpaca",
                        "timeframe": "1m",
                    }
                    result = await server._handle_subscribe(payload, mock_writer)
                    assert result is None
                    assert any(
                        b"Broker" in c.args[0] for c in mock_writer.write.call_args_list
                    )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_timeframe(self, server, mock_writer):
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value=set()
                    ):
                        payload = {
                            "symbol": "AAPL",
                            "market_type": "stocks",
                            "broker": "alpaca",
                            "timeframe": "m1",
                        }
                        result = await server._handle_subscribe(payload, mock_writer)
                        assert result is None
                        assert any(
                            b"Timeframe" in c.args[0]
                            for c in mock_writer.write.call_args_list
                        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_success_no_start(self, server, mock_writer):
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        payload = {
                            "symbol": "AAPL",
                            "market_type": "stocks",
                            "broker": "alpaca",
                            "timeframe": "1m",
                        }
                        result = await server._handle_subscribe(payload, mock_writer)

                        assert result is not None
                        assert isinstance(result, SocketConnection)
                        assert result.symbol == "AAPL"
                        assert result.market_type == MarketType.STOCKS
                        assert result.broker == BrokerType.ALPACA
                        assert result.timeframe == Timeframe.m1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_success_with_start_no_data(
        self, server, mock_writer
    ):
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        with patch.object(
                            server, "_fetch_ohlc", AsyncMock(return_value=[])
                        ):
                            payload = {
                                "symbol": "AAPL",
                                "market_type": "stocks",
                                "broker": "alpaca",
                                "timeframe": "1m",
                                "start": 1500000000,
                            }
                            result = await server._handle_subscribe(
                                payload, mock_writer
                            )

                            assert result is not None
                            assert (
                                result._replay_data is None
                            )  # Bootstrapped to live, no data to be fetched

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_success_with_start_has_data(
        self, server, mock_writer
    ):
        mock_row = make_ohlc_row()
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        with patch.object(
                            server, "_fetch_ohlc", AsyncMock(return_value=[mock_row])
                        ):
                            with patch.object(
                                server, "_send_next_replay_frame", AsyncMock()
                            ) as mock_send:
                                payload = {
                                    "symbol": "AAPL",
                                    "market_type": "stocks",
                                    "broker": "alpaca",
                                    "timeframe": "1m",
                                    "start": 1500000000,
                                }
                                result = await server._handle_subscribe(
                                    payload, mock_writer
                                )

                                assert result is not None
                                assert result._replay_data == [mock_row]
                                assert result._replay_idx == 0
                                mock_send.assert_awaited_once_with(result)


class TestOHLCFeedServerHandleReplayAck:
    """Unit tests for OHLCFeedServer._handle_replay_ack."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_replay_ack_no_replay_data(self, server, socket_conn):
        socket_conn._replay_data = None
        # Should not raise
        await server._handle_replay_ack(socket_conn)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_replay_ack_within_page(self, server, socket_conn):
        socket_conn._replay_data = [make_ohlc_row(), make_ohlc_row()]
        socket_conn._replay_idx = 0

        with patch.object(server, "_send_next_replay_frame", AsyncMock()) as mock_send:
            await server._handle_replay_ack(socket_conn)
            mock_send.assert_awaited_once_with(socket_conn)
            assert socket_conn._replay_idx == 0  # _send_next_replay_frame advances it

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_replay_ack_page_exhausted_has_next_page(
        self, server, socket_conn
    ):
        row1 = make_ohlc_row(timestamp=1500000000)
        socket_conn._replay_data = [row1]
        socket_conn._replay_idx = 1  # Past the only element
        socket_conn.timeframe = Timeframe.m1

        row2 = make_ohlc_row(timestamp=1500000060)
        with patch.object(
            server, "_fetch_ohlc", AsyncMock(return_value=[row2])
        ) as mock_fetch:
            with patch.object(
                server, "_send_next_replay_frame", AsyncMock()
            ) as mock_send:
                await server._handle_replay_ack(socket_conn)

                mock_fetch.assert_awaited_once()
                assert socket_conn._replay_data == [row2]
                assert socket_conn._replay_idx == 0
                mock_send.assert_awaited_once_with(socket_conn)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_replay_ack_page_exhausted_no_more_data(
        self, server, socket_conn
    ):
        row1 = make_ohlc_row(timestamp=1500000000)
        socket_conn._replay_data = [row1]
        socket_conn._replay_idx = 1
        socket_conn.timeframe = Timeframe.m1

        with patch.object(
            server, "_fetch_ohlc", AsyncMock(return_value=[])
        ) as mock_fetch:
            with patch.object(server, "_register_live") as mock_register:
                await server._handle_replay_ack(socket_conn)

                mock_fetch.assert_awaited_once()
                assert socket_conn._replay_data is None
                assert socket_conn._replay_idx == 0
                mock_register.assert_called_once_with(socket_conn)


class TestOHLCFeedServerRegisterLive:
    """Unit tests for OHLCFeedServer._register_live."""

    def test_register_live_adds_to_live_conns(self, server, socket_conn):
        server._register_live(socket_conn)

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert socket_conn in conns

    def test_register_live_multiple_connections(self, server, socket_conn, mock_writer):
        conn2 = SocketConnection(
            writer=mock_writer,
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )
        server._register_live(socket_conn)
        server._register_live(conn2)

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert len(conns) == 2
        assert socket_conn in conns
        assert conn2 in conns


class TestOHLCFeedServerFetchOhlc:
    """Unit tests for OHLCFeedServer._fetch_ohlc."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_ohlc_returns_records(self, server, socket_conn):
        mock_row1 = make_ohlc_row(timestamp=1500000000)
        mock_row2 = make_ohlc_row(timestamp=1500000060)

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_row1, mock_row2]

        mock_sess = AsyncMock()
        mock_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.markets.feed.server.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await server._fetch_ohlc(1500000000, socket_conn)

            assert len(result) == 2
            assert result[0].timestamp == 1500000000
            assert result[1].timestamp == 1500000060

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_ohlc_empty_result(self, server, socket_conn):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []

        mock_sess = AsyncMock()
        mock_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.markets.feed.server.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            result = await server._fetch_ohlc(1500000000, socket_conn)

            assert result == []

    @pytest.mark.asyncio(loop_scope="session")
    async def test_fetch_ohlc_respects_page_limit(self, server, socket_conn):
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []

        mock_sess = AsyncMock()
        mock_sess.execute = AsyncMock(return_value=mock_result)

        with patch("module.markets.feed.server.get_db_session") as mock_get_db:
            mock_get_db.return_value.__aenter__ = AsyncMock(return_value=mock_sess)
            mock_get_db.return_value.__aexit__ = AsyncMock(return_value=None)

            await server._fetch_ohlc(1500000000, socket_conn)

            # Verify the query has limit set to _REPLAY_PAGE
            call_args = mock_sess.execute.await_args
            stmt = call_args.args[0]
            # SQLAlchemy compile to check limit
            compiled = stmt.compile(compile_kwargs={"literal_binds": True})
            assert (
                str(server._REPLAY_PAGE) in str(compiled)
                or "LIMIT" in str(compiled).upper()
            )


class TestIntegration:
    """Integration-style tests for OHLCFeedServer."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_full_subscription_flow(self, server, mock_writer):
        """Test subscribe -> replay -> live bootstrap flow."""
        # Setup feed manager mocks
        with patch.object(feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        # Subscribe without start -> live
                        payload = {
                            "symbol": "AAPL",
                            "market_type": "stocks",
                            "broker": "alpaca",
                            "timeframe": "1m",
                        }
                        conn = await server._handle_subscribe(payload, mock_writer)

                        assert conn is not None
                        assert conn._replay_data is None

                        # Verify connection is in live set
                        live_set = server._live_conns["AAPL"][MarketType.STOCKS][
                            BrokerType.ALPACA
                        ][Timeframe.m1]
                        assert conn in live_set

    @pytest.mark.asyncio(loop_scope="session")
    async def test_broadcast_to_live_connections(self, server):
        """Test that handle_candle fans out to live connections."""
        writer1 = MagicMock(spec=asyncio.StreamWriter)
        writer1.get_extra_info = MagicMock(return_value=("10.0.0.1", 11111))
        writer1.write = MagicMock()
        writer1.drain = AsyncMock()

        writer2 = MagicMock(spec=asyncio.StreamWriter)
        writer2.get_extra_info = MagicMock(return_value=("10.0.0.2", 22222))
        writer2.write = MagicMock()
        writer2.drain = AsyncMock()

        conn1 = SocketConnection(
            writer=writer1,
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )
        conn2 = SocketConnection(
            writer=writer2,
            symbol="AAPL",
            market_type=MarketType.STOCKS,
            broker=BrokerType.ALPACA,
            timeframe=Timeframe.m1,
        )

        server._register_live(conn1)
        server._register_live(conn2)

        candle = make_ohlc_model(open=300.0, close=305.0)
        await server.handle_candle(candle)

        # Both writers should have received data
        writer1.write.assert_called_once()
        writer2.write.assert_called_once()

        # Verify payload
        data1 = json.loads(writer1.write.call_args.args[0].decode().strip())
        assert data1["candle"]["open"] == 300.0
        assert data1["is_live"] is True
