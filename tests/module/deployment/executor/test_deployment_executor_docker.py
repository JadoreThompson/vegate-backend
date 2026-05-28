from unittest.mock import MagicMock
from uuid import UUID

import pytest
from docker import DockerClient

from module.deployment.exception import DeploymentAlreadyRunningException, DeploymentNotFoundException
from module.deployment.executor.docker import DockerDeploymentExecutor
from module.deployment.executor.exception import DeploymentExistsException


@pytest.fixture
def mock_docker_client():
    return MagicMock(spec=DockerClient)


@pytest.fixture
def mock_deployment_id():
    return UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def image_name():
    return "vegate-deployment-test:latest"


class TestRunDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_deployment_starts_docker_container(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        mock_container = MagicMock()
        mock_container.id = "123"

        mock_docker_client.containers.list.return_value = []
        mock_docker_client.containers.create.return_value = mock_container

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )

        result = await executor.run(mock_deployment_id)

        mock_docker_client.containers.create.assert_called_once()
        mock_container.start.assert_called_once()

        _, kwargs = mock_docker_client.containers.create.call_args

        assert kwargs["image"] == image_name
        assert kwargs["name"] == f"dp_{mock_deployment_id}"
        assert (
            kwargs["command"] == f"deployment run --deployment-id {mock_deployment_id}"
        )
        assert kwargs["network"] == "vegate_network"

        assert kwargs["labels"] == {"deployment_id": str(mock_deployment_id)}, kwargs[
            "labels"
        ]

        assert result == {
            "deployment_id": str(mock_deployment_id),
            "status": "started",
            "container_id": mock_container.id,
        }

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_deployment_throws_exception_if_container_running(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_container.id = "123"

        mock_docker_client.containers.list.return_value = [mock_container]

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )
        
        with pytest.raises(DeploymentAlreadyRunningException) as exc:
            await executor.run(mock_deployment_id)

        assert mock_docker_client.containers.create.call_count == 0
        assert mock_container.start.call_count == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_deployment_removes_existing_stopped_container(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        existing_container = MagicMock()
        existing_container.status = "exited"

        new_container = MagicMock()
        new_container.id = "456"

        mock_docker_client.containers.list.return_value = [existing_container]
        mock_docker_client.containers.create.return_value = new_container

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )

        await executor.run(mock_deployment_id)

        existing_container.remove.assert_called_once_with(force=True)

        new_container.start.assert_called_once()


class TestStopDeployment:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_deployment_stops_and_removes_container(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        mock_container = MagicMock()

        mock_docker_client.containers.list.return_value = [mock_container]

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )

        result = await executor.stop(mock_deployment_id)

        mock_container.stop.assert_called_once_with(timeout=10)
        mock_container.remove.assert_called_once_with(force=True)

        assert result == {
            "deployment_id": str(mock_deployment_id),
            "status": "stopped",
        }

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_backtest_no_container_found(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        mock_docker_client.containers.list.return_value = []

        executor = DockerDeploymentExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )

        with pytest.raises(DeploymentNotFoundException) as exc:
            await executor.stop(mock_deployment_id)

        assert mock_docker_client.containers.list.call_count == 1

        _, kwargs = mock_docker_client.containers.list.call_args
        assert kwargs["filters"] == {"name": f"dp_{mock_deployment_id}"}
        assert kwargs["all"] is True

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_deployment_raises_if_container_not_found(
        self, mock_docker_client, mock_deployment_id, image_name
    ):
        mock_docker_client.containers.list.return_value = []

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )

        with pytest.raises(DeploymentNotFoundException):
            await executor.stop(mock_deployment_id)

        assert mock_docker_client.containers.list.call_count == 1

        _, kwargs = mock_docker_client.containers.list.call_args

        assert kwargs["filters"] == {"name": f"dp_{mock_deployment_id}"}

        assert kwargs["all"] is True


class TestStopAllDeployments:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_all_stops_and_removes_all_containers(
        self, mock_docker_client, image_name
    ):
        container_1 = MagicMock()
        container_1.status = "running"
        container_1.name = "dp_1"

        container_2 = MagicMock()
        container_2.status = "exited"
        container_2.name = "dp_2"

        mock_docker_client.containers.list.return_value = [
            container_1,
            container_2,
        ]

        executor = DockerDeploymentExecutor(
            image_name=image_name,
            docker_client=mock_docker_client,
        )

        result = await executor.stop_all()

        container_1.stop.assert_called_once_with(timeout=10)
        container_1.remove.assert_called_once_with(force=True)

        container_2.stop.assert_not_called()
        container_2.remove.assert_called_once_with(force=True)

        assert result == {
            "status": "stopped_all",
            "deployments": ["1", "2"],
        }
