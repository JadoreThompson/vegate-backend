from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from service.deployment.process import ProcessDeploymentService

BACKTEST_ID = UUID("11111111-1111-1111-1111-111111111111")
DEPLOYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def service():
    return ProcessDeploymentService()


@pytest.fixture
def mock_process():
    mock = MagicMock()
    mock.is_alive.return_value = True
    return mock


class TestDeployBacktest:

    @pytest.mark.asyncio
    async def test_deploy_backtest_starts_process(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            result = await service.deploy_backtest(BACKTEST_ID)

            MockProcessClass.assert_called_once()
            mock_process.start.assert_called_once()
            assert result == {"status": "deployed"}

    @pytest.mark.asyncio
    async def test_deploy_backtest_already_running_returns_already_running(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process
            mock_process.start()

            result = await service.deploy_backtest(BACKTEST_ID)
            result2 = await service.deploy_backtest(BACKTEST_ID)

            assert result2 == {"status": "already running"}


class TestStopBacktest:

    @pytest.mark.asyncio
    async def test_stop_backtest_terminates_running_process(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.deploy_backtest(BACKTEST_ID)
            result = await service.stop_backtest(BACKTEST_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)
            assert result == {"status": "stopped"}

    @pytest.mark.asyncio
    async def test_stop_backtest_not_running_returns_not_running(self, service):
        result = await service.stop_backtest(BACKTEST_ID)

        assert result == {"status": "not running"}

    @pytest.mark.asyncio
    async def test_stop_backtest_already_terminated_returns_not_running(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.deploy_backtest(BACKTEST_ID)
            mock_process.is_alive.return_value = False
            result = await service.stop_backtest(BACKTEST_ID)

            assert result == {"status": "not running"}


class TestDeployStrategy:

    @pytest.mark.asyncio
    async def test_deploy_strategy_starts_process(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            result = await service.deploy_strategy(DEPLOYMENT_ID)

            MockProcessClass.assert_called_once()
            mock_process.start.assert_called_once()
            assert result == {"status": "deployed"}

    @pytest.mark.asyncio
    async def test_deploy_strategy_already_running_returns_already_running(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process
            mock_process.start()

            result = await service.deploy_strategy(DEPLOYMENT_ID)
            result2 = await service.deploy_strategy(DEPLOYMENT_ID)

            assert result2 == {"status": "already running"}


class TestStopStrategy:
    @pytest.mark.asyncio
    async def test_stop_strategy_terminates_running_process(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.deploy_strategy(DEPLOYMENT_ID)
            result = await service.stop(DEPLOYMENT_ID)

            mock_process.terminate.assert_called_once()
            mock_process.join.assert_called_once_with(timeout=5)
            assert result == {"status": "stopped"}

    @pytest.mark.asyncio
    async def test_stop_strategy_not_running_returns_not_running(self, service):
        result = await service.stop(DEPLOYMENT_ID)

        assert result == {"status": "not running"}

    @pytest.mark.asyncio
    async def test_stop_strategy_already_terminated_returns_not_running(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.deploy_strategy(DEPLOYMENT_ID)
            mock_process.is_alive.return_value = False
            result = await service.stop(DEPLOYMENT_ID)

            assert result == {"status": "not running"}


class TestStopAll:

    @pytest.mark.asyncio
    async def test_stop_all_terminates_all_running_processes(self, service, mock_process):
        with patch("service.deployment.process.Process") as MockProcessClass:
            MockProcessClass.return_value = mock_process

            await service.deploy_backtest(BACKTEST_ID)
            await service.deploy_strategy(DEPLOYMENT_ID)
            result = await service.stop_all()

            assert mock_process.terminate.call_count == 2
            assert result == {"status": "all stopped"}

    @pytest.mark.asyncio
    async def test_stop_all_handles_no_running_processes(self, service):
        result = await service.stop_all()

        assert result == {"status": "all stopped"}
