from redis.asyncio import Redis as AsyncRedis
from starlette.types import ASGIApp, Scope, Receive, Send


class RateLimitMiddleware:

    def __init__(
        self,
        app: ASGIApp,
        redis_client: AsyncRedis,
        limit: int = 1000,
        window: int = 60,
    ):
        self.app = app
        self._redis_client = redis_client
        self.limit = limit
        self.window = window

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        client = scope.get("client")
        host = client[0] if client else None
        if not host:
            await self._send_error(send, 400, "Client host not found")
            return

        key = f"rate_limit:{host}"
        count = await self._redis_client.incr(key)
        if count == 1:
            await self._redis_client.expire(key, self.window)

        if count > self.limit:
            await self._send_error(send, 429, "Rate limit exceeded")
            return

        await self.app(scope, receive, send)

    async def _send_error(
        self, send: Send, status_code: int, message: str, headers=None
    ):
        headers = headers or []
        body = f'{{"error": "{message}"}}'.encode()
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [(b"content-type", b"application/json")] + headers,
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": body,
            }
        )
