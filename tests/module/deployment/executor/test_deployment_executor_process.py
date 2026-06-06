from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from module.backtest.exception import BacktestInProgressException
from module.deployment.exception import (
    DeploymentAlreadyRunningException,
    DeploymentNotFoundException,
)
from module.deployment.executor import ProcessDeploymentExecutor
from module.deployment.executor.exception import DeploymentLimitReached

BACKTEST_ID = UUID("11111111-1111-1111-1111-111111111111")
DEPLOYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")
DEPLOYMENT_ID_2 = UUID("33333333-3333-3333-3333-333333333333")
DEPLOYMENT_ID_3 = UUID("44444444-4444-4444-4444-444444444444")
PROCESS_PATCH_TARGET = "module.deployment.executor.process.Process"


@pytest.fixture
def executor():
    return ProcessDeploymentExecutor()


@pytest.fixture
def mock_process():
    mock = MagicMock()
    mock.is_alive.return_value = True
    return mock

def create_mock_process():
    mock = MagicMock()
    mock.is_alive.return_value = True
    return mock


class TestRunDeployment:

    @pytest.mark.asyncio
    async def test_run_deployment_starts_process(self, executor, mock_process):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            result = await executor.run(BACKTEST_ID)

            MockProcessClass.assert_called_once()
            mock_process.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_deployment_already_throws_exception_running(
        self, executor, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process
            mock_process.start()

            result = await executor.run(DEPLOYMENT_ID)

        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            with pytest.raises(DeploymentAlreadyRunningException):
                result2 = await executor.run(DEPLOYMENT_ID)
            
            assert MockProcessClass.call_count == 0


class TestStopDeployment:

    @pytest.mark.asyncio
    async def test_stop_deployment_terminates_running_process(
        self, executor, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await executor.run(DEPLOYMENT_ID)
            result = await executor.stop(DEPLOYMENT_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)

    @pytest.mark.asyncio
    async def test_stop_deployment_not_running_returns_not_running(self, executor):
        with pytest.raises(DeploymentNotFoundException):
            result = await executor.stop(DEPLOYMENT_ID)

    @pytest.mark.asyncio
    async def test_stop_deployment_already_terminated_returns_not_running(
        self, executor, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await executor.run(DEPLOYMENT_ID)
            mock_process.is_alive.return_value = False
            result = await executor.stop(DEPLOYMENT_ID)

class TestStopStrategy:
    @pytest.mark.asyncio
    async def test_stop_strategy_terminates_running_process(
        self, executor, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await executor.run(DEPLOYMENT_ID)
            result = await executor.stop(DEPLOYMENT_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)

    @pytest.mark.asyncio
    async def test_stop_strategy_not_running_returns_not_running(self, executor):
        with pytest.raises(DeploymentNotFoundException):
            result = await executor.stop(DEPLOYMENT_ID)

    @pytest.mark.asyncio
    async def test_stop_strategy_already_terminated_returns_not_running(
        self, executor, mock_process
    ):
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await executor.run(DEPLOYMENT_ID)
            mock_process.is_alive.return_value = False
            result = await executor.stop(DEPLOYMENT_ID)


class TestStopAll:

    @pytest.mark.asyncio
    async def test_stop_all_terminates_all_running_processes(
        self, executor
    ):
        mock_process_1 = create_mock_process()
        mock_process_2 = create_mock_process()
        executor.max_concurrent_deployments = 2
        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            MockProcessClass.side_effect = [mock_process_1, mock_process_2]

            await executor.run(DEPLOYMENT_ID)
            await executor.run(uuid4())
            result = await executor.stop_all()

            assert mock_process_1.terminate.call_count == 1
            assert mock_process_2.terminate.call_count == 1

    @pytest.mark.asyncio
    async def test_stop_all_handles_no_running_processes(self, executor):
        result = await executor.stop_all()


class TestConcurrencyLimit:

    @pytest.mark.asyncio
    async def test_raises_limit_reached_when_max_exceeded(self, executor):
        executor.max_concurrent_deployments = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            first_process = create_mock_process()
            second_process = create_mock_process()
            MockProcessClass.side_effect = [first_process, second_process]

            await executor.run(DEPLOYMENT_ID)
            await executor.run(DEPLOYMENT_ID_2)

            with pytest.raises(DeploymentLimitReached):
                await executor.run(DEPLOYMENT_ID_3)

    @pytest.mark.asyncio
    async def test_concurrency_limit_respected_after_stop_and_readd(self, executor):
        executor.max_concurrent_deployments = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcessClass:
            first_process = create_mock_process()
            second_process = create_mock_process()
            third_process = create_mock_process()
            MockProcessClass.side_effect = [
                first_process, second_process, third_process,
            ]

            await executor.run(DEPLOYMENT_ID)
            await executor.run(DEPLOYMENT_ID_2)

            with pytest.raises(DeploymentLimitReached):
                await executor.run(DEPLOYMENT_ID_3)

            await executor.stop(DEPLOYMENT_ID)

            await executor.run(DEPLOYMENT_ID_3)
            assert executor._deployments[DEPLOYMENT_ID_3] is third_process

            with pytest.raises(DeploymentLimitReached):
                await executor.run(uuid4())
