from typing import Generic, TypeVar

from engine.backtesting.ohlcv_loaders import BaseOHLCVLoader
from engine.brokers import BaseBroker
from engine.core import OHLCV


T = TypeVar("T", bound=BaseBroker)


class StrategyContext(Generic[T]):
    def __init__(self, broker: T, ohlcv_loader: BaseOHLCVLoader, candle: OHLCV | None = None):
        self._broker = broker
        self._timeframe = ohlcv_loader.timeframe
        self._current_candle = candle
        self._ohlcv_loader = ohlcv_loader

    @property
    def broker(self) -> T:
        return self._broker

    @property
    def timeframe(self):
        return self._timeframe

    @property
    def ohlcv_loader(self):
        return self._ohlcv_loader
    
    @property
    def current_candle(self):
        return self._current_candle
