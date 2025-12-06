from abc import ABC, abstractmethod

from .context import StrategyContext


class BaseStrategy(ABC):
    @abstractmethod
    def process(self, context: StrategyContext): ...

    def startup(self):
        return

    def shutdown(self):
        return
