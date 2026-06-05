from unittest.mock import MagicMock, patch

import pytest

from module.backtest.executor import (
    BacktestExecutorFactory,
    DockerBacktestExecutor,
    ProcessBacktestExecutor,
)


@pytest.fixture(autouse=True)
def clear_factory_cache():
    BacktestExecutorFactory._executors.clear()
    yield


class TestCreateProcessExecutor:

    def test_returns_process_executor(self):
        executor = BacktestExecutorFactory.create("process")
        assert isinstance(executor, ProcessBacktestExecutor)

    def test_is_cached(self):
        executor1 = BacktestExecutorFactory.create("process")
        executor2 = BacktestExecutorFactory.create("process")
        assert executor1 is executor2


class TestCreateDockerExecutor:

    @patch("module.backtest.executor.factory.sys.platform", "linux")
    def test_uses_unix_socket_on_linux(self):
        with patch("module.backtest.executor.factory.docker.DockerClient") as MockDockerClient:
            mock_client = MagicMock()
            MockDockerClient.return_value = mock_client

            executor = BacktestExecutorFactory.create("docker")

        assert isinstance(executor, DockerBacktestExecutor)
        MockDockerClient.assert_called_once_with(
            base_url="unix://var/run/docker.sock"
        )

    @patch("module.backtest.executor.factory.sys.platform", "win32")
    def test_uses_from_env_on_windows(self):
        with patch("module.backtest.executor.factory.docker.from_env") as mock_from_env:
            mock_client = MagicMock()
            mock_from_env.return_value = mock_client

            executor = BacktestExecutorFactory.create("docker")

        assert isinstance(executor, DockerBacktestExecutor)
        mock_from_env.assert_called_once()

    @patch("module.backtest.executor.factory.sys.platform", "linux")
    def test_is_cached(self):
        with patch("module.backtest.executor.factory.docker.DockerClient") as MockDockerClient:
            mock_client = MagicMock()
            MockDockerClient.return_value = mock_client

            executor1 = BacktestExecutorFactory.create("docker")
            executor2 = BacktestExecutorFactory.create("docker")

        assert executor1 is executor2
        MockDockerClient.assert_called_once()

    @patch("module.backtest.executor.factory.sys.platform", "darwin")
    def test_unknown_platform_raises_error(self):
        with pytest.raises(ValueError, match="Unknown platform 'darwin'"):
            BacktestExecutorFactory.create("docker")


class TestCreateUnsupportedExecutor:

    def test_raises_value_error(self):
        with pytest.raises(ValueError, match="Executor name 'invalid' not supported"):
            BacktestExecutorFactory.create("invalid")
