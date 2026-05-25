import asyncio
import json
import pytest
import pytest_asyncio
from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock, call
from uuid import uuid4

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.schema import OHLC as OHLCModel
from module.markets.feed.base import OHLCFeed
from module.markets.feed.manager import FeedManager
from module.markets.feed.server import OHLCFeedServer, SocketConnection


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
    return OHLCFeedServer(FeedManager(), host="127.0.0.1", port=9000)


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
    return SocketConnection(writer=mock_writer)


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
        conn = SocketConnection(writer=mock_writer)
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

        with patch.object(server._feed_manager, "stop_all", AsyncMock()):
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
            # symbol="AAPL",
            # market_type=MarketType.STOCKS,
            # broker_type=BrokerType.ALPACA,
            # timeframe=Timeframe.m1,
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

        with patch.object(server._feed_manager, "register", AsyncMock()):
            await server.init([mock_feed])

        # Mock server._feed_manager lookups
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    server._feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        server._feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        reader = MagicMock(spec=asyncio.StreamReader)
                        reader.readline = AsyncMock(
                            side_effect=[
                                json.dumps(
                                    {
                                        "type": "subscribe",
                                        "instruments": [
                                            {
                                                "symbol": "AAPL",
                                                "market_type": "stocks",
                                                "broker_type": "alpaca",
                                                "timeframe": ["1m"],
                                            },
                                        ],
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
    async def test_handle_subscribe_missing_instruments(self, server, mock_writer):
        mock_existing_instrument = MagicMock(spec=set)
        payload = {"type": "subscribe"}
        mock_connection = MagicMock(spec=SocketConnection)
        result = await server._handle_subscribe(
            payload, mock_connection, mock_existing_instrument
        )
        assert result is None
        assert any(
            b"Missing 'instruments'" in c.args[0]
            for c in mock_connection.send.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_empty_instruments(self, server, mock_writer):
        mock_existing_instrument = MagicMock(spec=set)
        payload = {"type": "subscribe", "instruments": []}
        mock_connection = MagicMock(spec=SocketConnection)
        result = await server._handle_subscribe(
            payload, mock_connection, mock_existing_instrument
        )
        assert result is None
        assert any(
            b"non-empty list" in c.args[0] for c in mock_connection.send.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_bad_entry_missing_key(self, server, mock_writer):
        payload = {
            "instruments": [
                {"symbol": "AAPL", "market_type": "stocks"},
            ],
        }
        mock_existing_instrument = MagicMock(spec=set)
        mock_connection = MagicMock(spec=SocketConnection)
        result = await server._handle_subscribe(
            payload, mock_connection, mock_existing_instrument
        )
        assert result is None
        assert any(
            b"Bad instrument entry" in c.args[0]
            for c in mock_connection.send.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_bad_entry_invalid_enum(self, server, mock_writer):
        payload = {
            "instruments": [
                {
                    "symbol": "AAPL",
                    "market_type": "invalid_market",
                    "broker_type": "alpaca",
                    "timeframe": ["1m"],
                },
            ],
        }
        mock_existing_instrument = MagicMock(spec=set)
        mock_connection = MagicMock(spec=SocketConnection)
        result = await server._handle_subscribe(
            payload, mock_connection, mock_existing_instrument
        )
        assert result is None
        assert any(
            b"Bad instrument entry" in c.args[0]
            for c in mock_connection.send.call_args_list
        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_symbol(self, server, mock_writer):
        with patch.object(server._feed_manager, "get_symbols", return_value=set()):
            payload = {
                "instruments": [
                    {
                        "symbol": "AAPL",
                        "market_type": "stocks",
                        "broker_type": "alpaca",
                        "timeframe": ["1m"],
                    },
                ],
            }
            mock_existing_instrument = MagicMock(spec=set)
            mock_connection = MagicMock(spec=SocketConnection)
            result = await server._handle_subscribe(
                payload, mock_connection, mock_existing_instrument
            )
            assert result is None
            assert any(
                b"is not supported" in c.args[0]
                for c in mock_connection.send.call_args_list
            )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_market_type(self, server, mock_writer):
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(server._feed_manager, "get_market_types", return_value=set()):
                payload = {
                    "instruments": [
                        {
                            "symbol": "AAPL",
                            "market_type": "stocks",
                            "broker_type": "alpaca",
                            "timeframe": ["1m"],
                        },
                    ],
                }
                mock_existing_instrument = MagicMock(spec=set)
                mock_connection = MagicMock(spec=SocketConnection)
                result = await server._handle_subscribe(
                    payload, mock_connection, mock_existing_instrument
                )
                assert result is None
                assert any(
                    b"Market type" in c.args[0]
                    for c in mock_connection.send.call_args_list
                )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_broker(self, server, mock_writer):
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(server._feed_manager, "get_brokers", return_value=set()):
                    payload = {
                        "instruments": [
                            {
                                "symbol": "AAPL",
                                "market_type": "stocks",
                                "broker_type": "alpaca",
                                "timeframe": ["1m"],
                            },
                        ],
                    }
                    mock_existing_instrument = MagicMock(spec=set)
                    mock_connection = MagicMock(spec=SocketConnection)
                    result = await server._handle_subscribe(
                        payload, mock_connection, mock_existing_instrument
                    )
                    assert result is None
                    assert any(
                        b"Broker" in c.args[0]
                        for c in mock_connection.send.call_args_list
                    )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_unsupported_timeframe(self, server, mock_writer):
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    server._feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        server._feed_manager, "get_timeframes", return_value=set()
                    ):
                        payload = {
                            "instruments": [
                                {
                                    "symbol": "AAPL",
                                    "market_type": "stocks",
                                    "broker_type": "alpaca",
                                    "timeframe": ["1m"],
                                },
                            ],
                        }
                        mock_existing_instrument = MagicMock(spec=set)
                        mock_connection = MagicMock(spec=SocketConnection)
                        result = await server._handle_subscribe(
                            payload, mock_connection, mock_existing_instrument
                        )
                        assert result is None
                        assert any(
                            b"Timeframe" in c.args[0]
                            for c in mock_connection.send.call_args_list
                        )

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_success_single_instrument(
        self, server, mock_writer
    ):
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    server._feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        server._feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        payload = {
                            "instruments": [
                                {
                                    "symbol": "AAPL",
                                    "market_type": "stocks",
                                    "broker_type": "alpaca",
                                    "timeframe": ["1m"],
                                },
                            ],
                        }
                        mock_existing_instrument = MagicMock(spec=set)
                        mock_connection = MagicMock(spec=SocketConnection)
                        result = await server._handle_subscribe(
                            payload, mock_connection, mock_existing_instrument
                        )

                        assert result is not None

                        assert isinstance(result, set)
                        assert len(result) == 1
                        result = list(result)
                        assert result[0][0] == "AAPL"
                        assert result[0][1] == MarketType.STOCKS
                        assert result[0][2] == BrokerType.ALPACA
                        assert result[0][3] == Timeframe.m1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_handle_subscribe_success_multiple_instruments(
        self, server, mock_writer
    ):
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL", "MSFT"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    server._feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        server._feed_manager,
                        "get_timeframes",
                        return_value={Timeframe.m1, Timeframe.m5},
                    ):
                        payload = {
                            "instruments": [
                                {
                                    "symbol": "AAPL",
                                    "market_type": "stocks",
                                    "broker_type": "alpaca",
                                    "timeframe": ["1m", "5m"],
                                },
                                {
                                    "symbol": "MSFT",
                                    "market_type": "stocks",
                                    "broker_type": "alpaca",
                                    "timeframe": ["1m"],
                                },
                            ],
                        }
                        mock_existing_instrument = MagicMock(spec=set)
                        mock_connection = MagicMock(spec=SocketConnection)
                        result = await server._handle_subscribe(
                            payload, mock_connection, mock_existing_instrument
                        )

                        assert result is not None
                        assert isinstance(result, set)
                        assert len(result) == 3

                        expected = {
                            (
                                "AAPL",
                                MarketType.STOCKS,
                                BrokerType.ALPACA,
                                Timeframe.m1,
                            ),
                            (
                                "AAPL",
                                MarketType.STOCKS,
                                BrokerType.ALPACA,
                                Timeframe.m5,
                            ),
                            (
                                "MSFT",
                                MarketType.STOCKS,
                                BrokerType.ALPACA,
                                Timeframe.m1,
                            ),
                        }

                        assert result == expected


class TestOHLCFeedServerRegisterLive:
    """Unit tests for OHLCFeedServer._register_live."""

    def test_register_live_adds_to_live_conns(self, server, socket_conn):
        server._register_live(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, socket_conn
        )

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert socket_conn in conns

    def test_register_live_multiple_connections(self, server, socket_conn, mock_writer):
        conn2 = SocketConnection(writer=mock_writer)
        server._register_live(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, socket_conn
        )
        server._register_live(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, conn2
        )

        conns = server._live_conns["AAPL"][MarketType.STOCKS][BrokerType.ALPACA][
            Timeframe.m1
        ]
        assert len(conns) == 2
        assert socket_conn in conns
        assert conn2 in conns


class TestIntegration:
    """Integration-style tests for OHLCFeedServer."""

    @pytest.mark.asyncio(loop_scope="session")
    async def test_full_subscription_flow(self, server, mock_writer):
        """Test subscribe -> live bootstrap flow."""
        with patch.object(server._feed_manager, "get_symbols", return_value={"AAPL"}):
            with patch.object(
                server._feed_manager, "get_market_types", return_value={MarketType.STOCKS}
            ):
                with patch.object(
                    server._feed_manager, "get_brokers", return_value={BrokerType.ALPACA}
                ):
                    with patch.object(
                        server._feed_manager, "get_timeframes", return_value={Timeframe.m1}
                    ):
                        payload = {
                            "instruments": [
                                {
                                    "symbol": "AAPL",
                                    "market_type": "stocks",
                                    "broker_type": "alpaca",
                                    "timeframe": ["1m"],
                                },
                            ],
                        }
                        mock_connection = MagicMock(spec=SocketConnection)
                        conns = await server._handle_subscribe(
                            payload, mock_connection, set()
                        )

                        assert conns is not None
                        assert len(conns) == 1

                        live_set = server._live_conns["AAPL"][MarketType.STOCKS][
                            BrokerType.ALPACA
                        ][Timeframe.m1]
                        assert mock_connection in live_set

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

        conn1 = SocketConnection(writer=writer1)
        conn1.send = AsyncMock()
        conn2 = SocketConnection(writer=writer2)
        conn2.send = AsyncMock()

        server._register_live(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, conn1
        )
        server._register_live(
            "AAPL", MarketType.STOCKS, BrokerType.ALPACA, Timeframe.m1, conn2
        )

        candle = make_ohlc_model(open=300.0, close=305.0)
        await server.handle_candle(candle)

        # Both writers should have received data
        conn1.send.assert_awaited_once()
        conn2.send.assert_awaited_once()

        # Verify payload
        data1 = json.loads(conn1.send.call_args.args[0].decode().strip())
        assert data1["candle"]["open"] == 300.0
        assert data1["is_live"] is True
