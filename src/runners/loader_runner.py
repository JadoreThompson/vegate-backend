import asyncio
from datetime import date
from typing import NamedTuple, Type

from enums import Timeframe
from services.loaders import BaseLoader
from .base import BaseRunner


class LoaderConfig(NamedTuple):
    cls: Type[BaseLoader]
    symbol: str
    timeframe: Timeframe
    start_date: date
    end_date: date


class LoaderRunner(BaseRunner):
    def __init__(self, configs: list[LoaderConfig]):
        self._loader_configs = tuple(configs)

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        results = await asyncio.gather(
            *[
                loader_config.cls().load_candles(
                    loader_config.symbol,
                    loader_config.timeframe,
                    loader_config.start_date,
                    loader_config.end_date,
                )
                for loader_config in self._loader_configs
            ],
            return_exceptions=True,
        )

        excs = [res for res in results if isinstance(res, Exception)]
        if excs:
            raise ExceptionGroup("Exceptions occured during load_candles", excs)
