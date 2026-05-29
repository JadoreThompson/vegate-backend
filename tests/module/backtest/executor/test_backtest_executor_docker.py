from unittest.mock import MagicMock

import pytest
from docker import DockerClient

from module.backtest.exception import (
    BacktestInProgressException,
    BacktestNotFoundException,
)
from module.backtest.executor.docker import DockerBacktestExecutor


@pytest.fixture
def mock_docker_client():
    return MagicMock(spec=DockerClient)


@pytest.fixture
def mock_backtest_id():
    return "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def image_name():
    return "vegate-backtest-test:latest"


class TestRunBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_backtest_starts_docker_container(
        self, mock_docker_client, mock_backtest_id, image_name
    ):
        mock_container = MagicMock()
        mock_container.id = "123"
        mock_docker_client.containers.create.return_value = mock_container

        executor = DockerBacktestExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )
        result = await executor.run(mock_backtest_id)

        mock_docker_client.containers.create.assert_called_once()
        mock_container.start.assert_called_once()

        args, kwargs = mock_docker_client.containers.create.call_args
        assert args[0] == image_name
        assert kwargs["name"] == f"bt_{mock_backtest_id}"
        assert kwargs["network"] == "vegate_network"
        assert kwargs["command"] == f"backtest run --backtest-id {mock_backtest_id}"
        assert kwargs["labels"] == {"backtest_id": mock_backtest_id}

        assert result == {
            "backtest_id": mock_backtest_id,
            "status": "started",
            "container_id": mock_container.id,
        }

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_backtest_throws_exception_if_container_already_running(
        self, mock_docker_client, mock_backtest_id, image_name
    ):
        mock_container = MagicMock()
        mock_container.status = "running"
        mock_docker_client.containers.list.return_value = [mock_container]

        executor = DockerBacktestExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )

        with pytest.raises(
            BacktestInProgressException, match=f"Backtest is currently in progress."
        ) as exc_info:
            await executor.run(mock_backtest_id)

        assert mock_docker_client.containers.create.call_count == 0
        assert mock_container.start.call_count == 0

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_backtest_stops_and_removes_existing_container(
        self, mock_docker_client, mock_backtest_id, image_name
    ):
        mock_container = MagicMock()
        mock_container.status = "exited"
        mock_docker_client.containers.list.return_value = [mock_container]

        executor = DockerBacktestExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )
        await executor.run(mock_backtest_id)

        mock_container.stop.assert_called_once()
        mock_container.remove.assert_called_once()


class TestStopBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_backtest_stops_and_removes_container(
        self, mock_docker_client, mock_backtest_id, image_name
    ):
        mock_container = MagicMock()
        mock_docker_client.containers.list.return_value = [mock_container]

        executor = DockerBacktestExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )
        await executor.stop(mock_backtest_id)

        mock_container.stop.assert_called_once()
        mock_container.remove.assert_called_once()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_stop_backtest_no_container_found(
        self, mock_docker_client, mock_backtest_id, image_name
    ):
        mock_docker_client.containers.list.return_value = []

        executor = DockerBacktestExecutor(
            image_name=image_name, docker_client=mock_docker_client
        )

        with pytest.raises(BacktestNotFoundException) as exc:
            await executor.stop(mock_backtest_id)

        assert str(exc.value) == f"Backtest with id '{mock_backtest_id}' not found."

        assert mock_docker_client.containers.list.call_count == 1

        _, kwargs = mock_docker_client.containers.list.call_args
        assert kwargs["filters"] == {"name": f"bt_{mock_backtest_id}"}
        assert kwargs["all"] is True
