from uuid import UUID

from docker import DockerClient
from docker.models.containers import Container

from .base import DeploymentExecutor
from .exception import DeploymentLimitReached
from ..exception import DeploymentAlreadyRunningException, DeploymentNotFoundException


class DockerDeploymentExecutor(DeploymentExecutor):

    def __init__(self, image_name: str, docker_client: DockerClient):
        super().__init__()
        self._image_name = image_name
        self._docker_client = docker_client

    async def run(self, deployment_id: UUID) -> dict:
        container = self._find_container(deployment_id)

        if container:
            if container.status == "running":
                raise DeploymentAlreadyRunningException(deployment_id)
            
            container.stop()
            container.remove(force=True)
        elif self._count_backtests() >= self.max_concurrent_deployments:
            raise DeploymentLimitReached()

        container = self._create_container(deployment_id)
        container.start()
        container.reload()

        return {
            "deployment_id": str(deployment_id),
            "status": "started",
            "container_id": container.id,
        }

    async def stop(self, deployment_id: UUID) -> dict:
        container = self._find_container(deployment_id)

        if not container:
            raise DeploymentNotFoundException(deployment_id)

        container.stop(timeout=10)
        container.remove(force=True)

        return {
            "deployment_id": str(deployment_id),
            "status": "stopped",
        }

    async def stop_all(self) -> dict:
        containers: list[Container] = self._docker_client.containers.list(
            all=True,
            filters={"name": "dp_"},
        )

        stopped = []

        for container in containers:
            try:
                deployment_id = container.name.removeprefix("dp_")

                if container.status == "running":
                    container.stop(timeout=10)

                container.remove(force=True)
                stopped.append(deployment_id)

            except Exception:
                continue

        return {
            "status": "stopped_all",
            "deployments": stopped,
        }

    def _find_container(self, deployment_id: UUID) -> Container | None:
        containers = self._docker_client.containers.list(
            all=True,
            filters={"name": f"dp_{deployment_id}"},
        )

        return containers[0] if containers else None

    def _create_container(self, deployment_id: UUID) -> Container:
        return self._docker_client.containers.create(
            image=self._image_name,
            name=f"dp_{deployment_id}",
            network="vegate_network",
            command=f"uv run src/main.py deployment run --deployment-id {deployment_id}",
            labels={"deployment_id": str(deployment_id)},
        )

    def _count_backtests(self):
        return len(self._docker_client.containers.list(all=True, filters={"name": "dp_"}))
