from unittest.mock import MagicMock, patch

import pytest

from module.deployment.executor import (
    DeploymentExecutorFactory,
    DockerDeploymentExecutor,
    ProcessDeploymentExecutor,
)


@pytest.fixture(autouse=True)
def clear_factory_cache():
    DeploymentExecutorFactory._executors.clear()
    yield


class TestCreateProcessExecutor:

    def test_returns_process_executor(self):
        executor = DeploymentExecutorFactory.create("process")
        assert isinstance(executor, ProcessDeploymentExecutor)

    def test_is_cached(self):
        executor1 = DeploymentExecutorFactory.create("process")
        executor2 = DeploymentExecutorFactory.create("process")
        assert executor1 is executor2


class TestCreateDockerExecutor:

    @patch("module.deployment.executor.factory.sys.platform", "linux")
    def test_uses_unix_socket_on_linux(self):
        with patch("module.deployment.executor.factory.docker.DockerClient") as MockDockerClient:
            mock_client = MagicMock()
            MockDockerClient.return_value = mock_client

            executor = DeploymentExecutorFactory.create("docker")

        assert isinstance(executor, DockerDeploymentExecutor)
        MockDockerClient.assert_called_once_with(
            base_url="unix://var/run/docker.sock"
        )

    @patch("module.deployment.executor.factory.sys.platform", "win32")
    def test_uses_from_env_on_windows(self):
        with patch("module.deployment.executor.factory.docker.from_env") as mock_from_env:
            mock_client = MagicMock()
            mock_from_env.return_value = mock_client

            executor = DeploymentExecutorFactory.create("docker")

        assert isinstance(executor, DockerDeploymentExecutor)
        mock_from_env.assert_called_once()

    @patch("module.deployment.executor.factory.sys.platform", "linux")
    def test_is_cached(self):
        with patch("module.deployment.executor.factory.docker.DockerClient") as MockDockerClient:
            mock_client = MagicMock()
            MockDockerClient.return_value = mock_client

            executor1 = DeploymentExecutorFactory.create("docker")
            executor2 = DeploymentExecutorFactory.create("docker")

        assert executor1 is executor2
        MockDockerClient.assert_called_once()

    @patch("module.deployment.executor.factory.sys.platform", "darwin")
    def test_unknown_platform_raises_error(self):
        with pytest.raises(ValueError, match="Unknown platform 'darwin'"):
            DeploymentExecutorFactory.create("docker")


class TestCreateUnsupportedExecutor:

    def test_raises_value_error(self):
        with pytest.raises(ValueError, match="Executor name 'invalid' not supported"):
            DeploymentExecutorFactory.create("invalid")
