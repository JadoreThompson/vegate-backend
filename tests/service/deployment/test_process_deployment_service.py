from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from module.deployment.exception import DeploymentNotFoundException
from module.deployment.executor import ProcessDeploymentExecutor

BACKTEST_ID = UUID("11111111-1111-1111-1111-111111111111")
DEPLOYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")
PROCESS_PATCH_TARGET = "module.deployment.executor.process.Process"

@pytest.fixture
def service():
    return ProcessDeploymentExecutor()


@pytest.fixture
def mock_process():
    mock = MagicMock()
    mock.is_alive.return_value = True
    return mock


class TestDeployDeployment:

    @pytest.mark.asyncio
    async def test_deploy_deployment_starts_process(self, service, mock_process):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            result = await service.run(BACKTEST_ID)

            MockProcessClass.assert_called_once()
            mock_process.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_deploy_deployment_already_running_returns_already_running(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process
            mock_process.start()

            result = await service.run(DEPLOYMENT_ID)
            result2 = await service.run(DEPLOYMENT_ID)


class TestStopDeployment:

    @pytest.mark.asyncio
    async def test_stop_deployment_terminates_running_process(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.run(DEPLOYMENT_ID)
            result = await service.stop(DEPLOYMENT_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)

    @pytest.mark.asyncio
    async def test_stop_deployment_not_running_returns_not_running(self, service):
        with pytest.raises(DeploymentNotFoundException):
            result = await service.stop(DEPLOYMENT_ID)

    @pytest.mark.asyncio
    async def test_stop_deployment_already_terminated_returns_not_running(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.run(DEPLOYMENT_ID)
            mock_process.is_alive.return_value = False
            result = await service.stop(DEPLOYMENT_ID)


class TestDeployStrategy:

    @pytest.mark.asyncio
    async def test_run_starts_process(self, service, mock_process):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            result = await service.run(DEPLOYMENT_ID)

            MockProcessClass.assert_called_once()
            mock_process.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_already_running_returns_already_running(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process
            mock_process.start()

            result = await service.run(DEPLOYMENT_ID)
            result2 = await service.run(DEPLOYMENT_ID)


class TestStopStrategy:
    @pytest.mark.asyncio
    async def test_stop_strategy_terminates_running_process(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.run(DEPLOYMENT_ID)
            result = await service.stop(DEPLOYMENT_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)

    @pytest.mark.asyncio
    async def test_stop_strategy_not_running_returns_not_running(self, service):
        with pytest.raises(DeploymentNotFoundException):
            result = await service.stop(DEPLOYMENT_ID)

    @pytest.mark.asyncio
    async def test_stop_strategy_already_terminated_returns_not_running(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.run(DEPLOYMENT_ID)
            mock_process.is_alive.return_value = False
            result = await service.stop(DEPLOYMENT_ID)


class TestStopAll:

    @pytest.mark.asyncio
    async def test_stop_all_terminates_all_running_processes(
        self, service, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.run(DEPLOYMENT_ID)
            await service.run(DEPLOYMENT_ID)
            result = await service.stop_all()

            assert mock_process.terminate.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_all_handles_no_running_processes(self, service):
        result = await service.stop_all()
