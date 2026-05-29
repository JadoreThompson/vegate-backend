from multiprocessing import Process
from uuid import UUID

from core.redis import REDIS_CLIENT_SYNC
from module.event_bus import SyncEventPublisher
from .base import BacktestExecutor


def _run_backtest(backtest_id: UUID):
    from module.backtest.runner import BacktestRunner

    runner = BacktestRunner(backtest_id, event_publisher=SyncEventPublisher(), redis_client=REDIS_CLIENT_SYNC)
    runner.run()


class ProcessBacktestExecutor(BacktestExecutor):
    "Launches backtests in separate processes using multiprocessing.Process."

    def __init__(self):
        super().__init__()
        self._backtests: dict[UUID, Process] = {}

    @property
    def backtests(self) -> list[UUID]:
        return list(self._backtests.keys())

    async def run(self, backtest_id: UUID) -> dict:
        if len(self._backtests) >= self.max_concurrent_backtests:
            raise ValueError(f"Max concurrent backtests {self.max_concurrent_backtests} reached.")
        
        if backtest_id in self._backtests and self._backtests[backtest_id].is_alive():
            return

        p = Process(target=_run_backtest, args=(backtest_id,))
        p.start()
        self._backtests[backtest_id] = p
        return

    async def stop(self, backtest_id: UUID) -> dict:
        if (
                backtest_id not in self._backtests
                or not self._backtests[backtest_id].is_alive()
        ):
            return
        
        self._backtests[backtest_id].terminate()
        self._backtests[backtest_id].join(timeout=5)

        return

    async def stop_all(self) -> dict:
        for backtest_id, process in self._backtests.items():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        self._backtests.clear()
