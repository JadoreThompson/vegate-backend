from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from module.backtest.exception import BacktestInProgressException
from module.backtest.executor import ProcessBacktestExecutor

BACKTEST_ID = UUID("11111111-1111-1111-1111-111111111111")
BACKTEST_ID_2 = UUID("22222222-2222-2222-2222-222222222222")

PROCESS_PATCH_TARGET = "module.backtest.executor.process.Process"


def make_mock_process(is_alive: bool = True) -> MagicMock:
    """Return a fully configured mock process."""
    p = MagicMock()
    p.is_alive.return_value = is_alive
    return p


@pytest.fixture
def executor():
    return ProcessBacktestExecutor()


class TestDeployBacktest:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_run_backtest_starts_process(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            result = await executor.run(BACKTEST_ID)

            assert BACKTEST_ID in executor._backtests
            mock_process.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_backtest_already_throws_exception(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)
        
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            with pytest.raises(BacktestInProgressException):
                result = await executor.run(BACKTEST_ID)
            assert MockProcess.call_count == 0

    @pytest.mark.asyncio
    async def test_run_backtest_stores_process_reference(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)

            assert BACKTEST_ID in executor._backtests
            assert executor._backtests[BACKTEST_ID] is mock_process

    @pytest.mark.asyncio
    async def test_run_backtest_reuses_terminated_process(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            # Simulate the first process having exited since deployment.
            first_process.is_alive.return_value = False

            result = await executor.run(BACKTEST_ID)

            assert executor._backtests[BACKTEST_ID] is second_process
            second_process.start.assert_called_once()


class TestStopBacktest:

    @pytest.mark.asyncio
    async def test_stop_backtest_terminates_running_process(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)
            result = await executor.stop(BACKTEST_ID)

            mock_process.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_backtest_already_terminated_returns_not_running(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)
            mock_process.is_alive.return_value = False

            result = await executor.stop(BACKTEST_ID)

            mock_process.terminate.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_backtest_does_not_remove_process_from_tracking(self, executor):
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)
            await executor.stop(BACKTEST_ID)

            # The entry should still be present (stopped, but tracked).
            assert BACKTEST_ID in executor._backtests


class TestStopAll:

    @pytest.mark.asyncio
    async def test_stop_all_terminates_all_running_processes(self, executor):
        executor.max_concurrent_backtests = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            await executor.run(BACKTEST_ID_2)
            result = await executor.stop_all()

            first_process.terminate.assert_called_once()
            second_process.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_all_waits_for_processes_to_join(self, executor):
        executor.max_concurrent_backtests = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            await executor.run(BACKTEST_ID_2)
            await executor.stop_all()

            first_process.join.assert_called_once_with(timeout=5)
            second_process.join.assert_called_once_with(timeout=5)

    @pytest.mark.asyncio
    async def test_stop_all_skips_already_terminated_processes(self, executor):
        executor.max_concurrent_backtests = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=False)  # already dead
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            await executor.run(BACKTEST_ID_2)
            await executor.stop_all()

            first_process.terminate.assert_called_once()
            second_process.terminate.assert_not_called()


class TestBacktestProcessRunner:

    @pytest.mark.asyncio
    async def test_run_backtest_stores_process_with_correct_target(self, executor):
        from module.backtest.executor.process import _run_backtest

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            mock_process = MockProcess.return_value
            mock_process.is_alive.return_value = True

            await executor.run(BACKTEST_ID)

            # Verify Process was constructed with _run_backtest as its target.
            constructor_kwargs = MockProcess.call_args.kwargs
            assert (
                constructor_kwargs.get("target") is _run_backtest
            ), "Process should be created with _run_backtest as the target function"


class TestMultipleBacktests:

    @pytest.mark.asyncio
    async def test_deploy_multiple_different_backtests(self, executor):
        executor.max_concurrent_backtests = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            result1 = await executor.run(BACKTEST_ID)
            result2 = await executor.run(BACKTEST_ID_2)

            assert executor._backtests[BACKTEST_ID] is first_process
            assert executor._backtests[BACKTEST_ID_2] is second_process

    @pytest.mark.asyncio
    async def test_stop_one_backtest_does_not_affect_others(self, executor):
        executor.max_concurrent_backtests = 2

        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            await executor.run(BACKTEST_ID_2)

            result = await executor.stop(BACKTEST_ID)

            first_process.terminate.assert_called_once()
            # Second process must be completely untouched.
            second_process.terminate.assert_not_called()
            second_process.join.assert_not_called()
            assert second_process.is_alive()

    @pytest.mark.asyncio
    async def test_stop_all_terminates_every_running_backtest(self, executor):
        executor.max_concurrent_backtests = 2
        
        with patch(PROCESS_PATCH_TARGET) as MockProcess:
            first_process = make_mock_process(is_alive=True)
            second_process = make_mock_process(is_alive=True)
            MockProcess.side_effect = [first_process, second_process]

            await executor.run(BACKTEST_ID)
            await executor.run(BACKTEST_ID_2)
            await executor.stop_all()

            first_process.terminate.assert_called_once()
            first_process.join.assert_called_once_with(timeout=5)
            second_process.terminate.assert_called_once()
            second_process.join.assert_called_once_with(timeout=5)
