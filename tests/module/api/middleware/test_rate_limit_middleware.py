import pytest
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio

from core.redis import REDIS_CLIENT
from module.api.middleware.rate_limit import RateLimitMiddleware


@pytest.fixture
def mock_redis_client():
    return AsyncMock()


@pytest_asyncio.fixture(loop_scope="session")
async def redis_client():
    await REDIS_CLIENT.flushall()
    return REDIS_CLIENT


@pytest.fixture
def mock_asgi_app():
    return AsyncMock()


@pytest.fixture
def mock_scope():
    return AsyncMock()


@pytest.fixture
def mock_receive():
    return AsyncMock()


@pytest.fixture
def mock_send():
    return AsyncMock()


@pytest.mark.asyncio(loop_scope="session")
async def test_rate_limit_sends_error(
    mock_redis_client, redis_client, mock_asgi_app, mock_scope, mock_receive, mock_send
):
    limit = 5
    limiter = RateLimitMiddleware(
        app=mock_asgi_app,
        redis_client=redis_client,
        limit=limit,
        window=1,
    )

    def generate_mock_client():
        mock_client_get_item = MagicMock(return_value="ip-address")
        mock_client = MagicMock()
        mock_client.__getitem__ = mock_client_get_item

        mock_scope_get = MagicMock(return_value=mock_client)
        mock_scope.get = mock_scope_get

        mock_scope_get_item = MagicMock(return_value="http")
        mock_scope.__getitem__ = mock_scope_get_item

        return mock_client

    for _ in range(limit):
        mock_client = generate_mock_client()
        await limiter(scope=mock_scope, receive=mock_receive, send=mock_send)

        mock_scope.__getitem__.assert_called_once_with("type")
        mock_scope.get.assert_called_once_with("client")
        mock_client.__getitem__.assert_called_once_with(0)
        assert mock_send.await_count == 0

    await limiter(scope=mock_scope, receive=mock_receive, send=mock_send)

    start_call, body_call = mock_send.await_args_list

    arg = start_call.args[0]
    assert arg["type"] == "http.response.start"
    assert arg["status"] == 429
    assert arg["headers"] == [(b"content-type", b"application/json")]
    assert len(arg) == 3

    arg = body_call.args[0]
    assert arg["type"] == "http.response.body"
    assert arg["body"] == '{"error": "Rate limit exceeded"}'.encode()
    assert len(arg) == 2


@pytest.mark.asyncio(loop_scope="session")
async def test_rate_limit_resets_after_window(
    redis_client, mock_asgi_app, mock_scope, mock_receive, mock_send
):
    limit = 10
    window = 3

    limiter = RateLimitMiddleware(
        app=mock_asgi_app,
        redis_client=redis_client,
        limit=limit,
        window=window,
    )

    def generate_mock_scope():
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value="127.0.0.1")

        scope = MagicMock()
        scope.__getitem__ = MagicMock(return_value="http")
        scope.get = MagicMock(return_value=mock_client)
        return scope

    for _ in range(9):
        scope = generate_mock_scope()
        await limiter(scope=scope, receive=mock_receive, send=mock_send)
        assert mock_send.await_count == 0

    # simulate window reset
    await redis_client.flushall()

    for _ in range(2):
        scope = generate_mock_scope()
        await limiter(scope=scope, receive=mock_receive, send=mock_send)
        assert mock_send.await_count == 0

    assert mock_send.call_count == 0
