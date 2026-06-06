import asyncio

import aiohttp
import pytest
from aiohttp.test_utils import unused_port

from module.health.server import HealthCheckServer


class TestHealthCheckServer:


    @pytest.mark.asyncio(loop_scope="session")
    async def test_health_endpoint_returns_ok(self):
        port = unused_port()
        server = HealthCheckServer(host="127.0.0.1", port=port)

        task = asyncio.create_task(server.run_forever())
        await asyncio.sleep(0.2)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                    assert resp.status == 200
                    text = await resp.text()
                    assert text == "OK"
        finally:
            await server.stop()
            await task

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_terminates_run_forever(self):
        port = unused_port()
        server = HealthCheckServer(host="127.0.0.1", port=port)

        task = asyncio.create_task(server.run_forever())
        await asyncio.sleep(0.2)

        await server.stop()

        result = await task
        assert result is None
