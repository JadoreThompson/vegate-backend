from abc import ABC, abstractmethod
from .context import StrategyContext


class BaseStrategy(ABC):
    @abstractmethod
    def on_candle(self, context: StrategyContext): ...

    def startup(self):
        return

    def shutdown(self):
        return
