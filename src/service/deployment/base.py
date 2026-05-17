from abc import abstractmethod, ABC
from uuid import UUID


class DeploymentService(ABC):

    def __init__(self):
        pass

    async def init(self, *args, **kw) -> None:
        pass

    @abstractmethod
    async def run(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop(self, deployment_id: UUID) -> dict:
        pass

    @abstractmethod
    async def stop_all(self) -> dict:
        pass
