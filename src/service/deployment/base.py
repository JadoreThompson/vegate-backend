from abc import abstractmethod
from uuid import UUID


class DeploymentService:
    def __init__(self):
        pass

    @abstractmethod
    async def deploy_backtest(self, backtest_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop_backtest(self, backtest_id: UUID) -> dict:
        pass

    @abstractmethod
    async def deploy_strategy(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop_strategy(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop_all(self) -> dict:
        pass
