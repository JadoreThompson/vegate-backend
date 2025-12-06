from typing import Generic, TypeVar

from engine.brokers import BaseBroker
from engine.ohlcv import OHLCV


T = TypeVar("T", bound=BaseBroker)


class StrategyContext(Generic[T]):
    def __init__(self, broker: T, candle: OHLCV | None = None):
        self._broker = broker
        self._current_candle = candle

    @property
    def broker(self) -> T:
        return self._broker
    
    @property
    def current_candle(self):
        return self._current_candle
