from abc import ABC, abstractmethod
from uuid import UUID


class BacktestService(ABC):

    async def init(self, *args, **kwargs):
        pass

    @abstractmethod
    async def run(self, backtest_id: UUID):
        pass

    @abstractmethod
    async def stop(self, backtest_id: UUID):
        pass
