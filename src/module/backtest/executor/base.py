from abc import ABC, abstractmethod
from uuid import UUID


class BacktestExecutor(ABC):

    def __init__(self):
        self.max_concurrent_backtests = 1

    @abstractmethod
    async def run(self, backtest_id: UUID):
        pass

    @abstractmethod
    async def stop(self, backtest_id: UUID):
        pass
