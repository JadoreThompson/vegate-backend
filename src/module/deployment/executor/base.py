from abc import ABC, abstractmethod
from uuid import UUID


class DeploymentExecutor(ABC):

    def __init__(self):
        self.max_concurrent_deployments = 1

    @abstractmethod
    async def run(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop_all(self) -> dict:
        pass
