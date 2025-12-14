import asyncio

from engine.enums import MarketType
from pipelines import OHLCBuilder
from pipelines.listeners import AlpacaListener
from .base import BaseRunner


class ListenerRunner(BaseRunner):
    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        listener = AlpacaListener()
        listener.initialise()
        task = asyncio.create_task(listener.run())
        listener.listen(MarketType.CRYPTO, ["BTC/USD"])

        builder = OHLCBuilder()

        await asyncio.gather(task, builder.start())
