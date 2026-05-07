import asyncio

from service.ohlc.loader.loader_config import LoaderConfig
from .base import BaseRunner


class LoaderRunner(BaseRunner):
    def __init__(self, configs: list[LoaderConfig]):
        self._loader_configs = tuple(configs)

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        results = await asyncio.gather(
            *[
                loader_config.cls().run(loader_config)
                for loader_config in self._loader_configs
            ],
            return_exceptions=True,
        )

        excs = [res for res in results if isinstance(res, Exception)]
        if excs:
            raise ExceptionGroup("Exceptions occured during load_candles", excs)
