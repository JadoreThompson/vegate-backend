from uuid import UUID

from docker import DockerClient
from docker.errors import APIError as DockerAPIError
from docker.models.containers import Container

from config import OHLC_FEED_HOST, OHLC_FEED_PORT, OMS_BASE_URL

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

        if container is not None:
            if container.status == "running":
                raise DeploymentAlreadyRunningException(deployment_id)

            container.remove(force=True)
        elif self._count_deployments() >= self.max_concurrent_deployments:
            raise DeploymentLimitReached()

        container = self._create_container(deployment_id)

        try:
            container.start()
            container.reload()
        except DockerAPIError:
            container.reload()
            container.remove(force=True)
            raise

        return {
            "deployment_id": str(deployment_id),
            "status": "started",
            "container_id": container.id,
        }

    async def stop(self, deployment_id: UUID) -> dict:
        container = self._find_container(deployment_id)

        if container is None:
            raise DeploymentNotFoundException(deployment_id)
        
        self._stop_container(container)
        return {
            "deployment_id": str(deployment_id),
            "status": "stopped",
        }

    def _stop_container(self, container: Container) -> dict:
        if container.status == "running":
            container.stop(timeout=10)
        else:
            container.remove(force=True)

    async def stop_all(self) -> dict:
        containers: list[Container] = self._docker_client.containers.list(
            all=True,
            filters={"name": "dp_"},
        )

        stopped = []

        for container in containers:
            try:
                deployment_id = UUID(container.name.removeprefix("dp_"))
                self._stop_container(container)
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
            command=f"uv run src/main.py deployment run --deployment-id {deployment_id} --ohlc-feed-host {OHLC_FEED_HOST} --ohlc-feed-port {OHLC_FEED_PORT} --oms-base-url {OMS_BASE_URL}",
            labels={"deployment_id": str(deployment_id)},
            auto_remove=True,
        )

    def _count_deployments(self):
        return len(
            self._docker_client.containers.list(all=True, filters={"name": "dp_"})
        )
