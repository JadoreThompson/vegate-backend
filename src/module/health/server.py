import asyncio
from aiohttp import web


class HealthCheckServer:

    def __init__(self, host: str = "0.0.0.0", port: int = 5555):
        self._host = host
        self._port = port

        app = web.Application()
        app.router.add_get("/health", self._health)

        self._runner = web.AppRunner(app)
        self._event = asyncio.Event()

    @property
    def host(self) -> str:
        return self._host
    
    @property
    def port(self) -> int:
        return self._port

    async def run_forever(self):
        await self._runner.setup()

        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

        await self._event.wait()

    async def stop(self):
        await self._runner.cleanup()
        self._event.set()

    @staticmethod
    async def _health(_: web.Request):
        return web.Response(text="OK")
