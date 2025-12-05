from typing import Literal

from engine.backtesting.ohlcv_loaders import BaseOHLCVLoader, DBOHLCVLoader


OHLCVLoaderName = Literal["db"]


class OHLCVLoaderFactory:
    _loaders: dict[OHLCVLoaderName, BaseOHLCVLoader] = {"db": DBOHLCVLoader()}

    @classmethod
    def get(cls, name: Literal["db"]):
        if name not in cls._loaders:
            raise NotImplementedError(f"'{name}' loader not implemented.")
        return cls._loaders[name]
