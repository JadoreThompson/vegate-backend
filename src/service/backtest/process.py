from multiprocessing import Process
from uuid import UUID

from service.backtest.base import BacktestService


def _run_backtest(backtest_id: UUID):
    from runners.backtest_runner import BacktestRunner

    runner = BacktestRunner(backtest_id)
    runner.run()


class ProcessBacktestService(BacktestService):
    "Launches backtests in separate processes using multiprocessing.Process."

    def __init__(self):
        super().__init__()
        self._backtests: dict[UUID, Process] = {}
        self._deployments: dict[UUID, Process] = {}
        self._max_concurrent_backtests = 5

    @property
    def backtests(self) -> list[UUID]:
        return list(self._backtests.keys())

    @property
    def max_concurrent_backtests(self) -> int:
        return self._max_concurrent_backtests

    async def init(self, max_concurrent_backtests: int):
        self._max_concurrent_backtests = max_concurrent_backtests

    async def run(self, backtest_id: UUID) -> dict:
        if backtest_id in self._backtests and self._backtests[backtest_id].is_alive():
            return {"status": "already running"}

        p = Process(target=_run_backtest, args=(backtest_id,))
        p.start()
        self._backtests[backtest_id] = p
        return {"status": "deployed"}

    async def stop(self, backtest_id: UUID) -> dict:
        if (
                backtest_id not in self._backtests
                or not self._backtests[backtest_id].is_alive()
        ):
            return {"status": "not running"}
        self._backtests[backtest_id].terminate()
        self._backtests[backtest_id].join(timeout=5)
        return {"status": "stopped"}

    async def stop_all(self) -> dict:
        for backtest_id, process in self._backtests.items():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        return {"status": "all stopped"}
