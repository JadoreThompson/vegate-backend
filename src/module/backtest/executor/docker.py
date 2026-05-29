from uuid import UUID

from docker import DockerClient
from docker.models.containers import Container


from .base import BacktestExecutor
from .exception import BacktestLimitReached
from ..exception import BacktestInProgressException, BacktestNotFoundException


class DockerBacktestExecutor(BacktestExecutor):
    """
    Manages backtests using Docker containers
    """

    def __init__(self, image_name: str, docker_client: DockerClient):
        super().__init__()
        self._image_name = image_name
        self._docker_client = docker_client

    async def run(self, backtest_id: UUID):
        container = self._find_container(backtest_id)
        if container:
            if container.status == "running":
                raise BacktestInProgressException()
            
            container.stop()
            container.remove(force=True)
        elif self._count_backtests() >= self.max_concurrent_backtests:
            raise BacktestLimitReached()
        
        container = self._create_container(backtest_id)
        container.start()
        container.reload()

        return {
            "backtest_id": backtest_id,
            "status": "started",
            "container_id": container.id,
        }

    async def stop(self, backtest_id):
        container = self._find_container(backtest_id)

        if not container:
            raise BacktestNotFoundException(backtest_id)
        
        container.stop()
        container.remove()
        
        return {
            "backtest_id": str(backtest_id),
            "status": "stopped",
        }

    def _find_container(self, backtest_id: UUID) -> Container | None:
        containers = self._docker_client.containers.list(
            all=True, filters={"name": f"bt_{backtest_id}"}
        )
        return containers[0] if containers else None

    def _create_container(self, backtest_id: UUID) -> Container:
        container_name = f"bt_{str(backtest_id)}"
        container = self._docker_client.containers.create(
            self._image_name,
            name=container_name,
            network="vegate_network",
            command=f"backtest run --backtest-id {backtest_id}",
            labels={"backtest_id": str(backtest_id)},
        )
        return container
    
    def _count_backtests(self):
        return len(self._docker_client.containers.list(all=True, filters={"name": "bt_"}))
