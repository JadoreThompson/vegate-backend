from multiprocessing import Process
from uuid import UUID

from service.deployment.base import DeploymentService


def _run_backtest(backtest_id: UUID):
    from runners.backtest_runner import BacktestRunner

    runner = BacktestRunner(backtest_id)
    runner.run()


def _run_strategy(deployment_id: UUID):
    from runners.deployment_runner import DeploymentRunner

    runner = DeploymentRunner(deployment_id)
    runner.run()


class ProcessDeploymentService(DeploymentService):
    def __init__(self):
        super().__init__()
        self._backtests: dict[UUID, Process] = {}
        self._deployments: dict[UUID, Process] = {}

    async def deploy_backtest(self, backtest_id: UUID) -> dict:
        if backtest_id in self._backtests and self._backtests[backtest_id].is_alive():
            return {"status": "already running"}

        p = Process(target=_run_backtest, args=(backtest_id,))
        p.start()
        self._backtests[backtest_id] = p
        return {"status": "deployed"}

    async def stop_backtest(self, backtest_id: UUID) -> dict:
        if (
            backtest_id not in self._backtests
            or not self._backtests[backtest_id].is_alive()
        ):
            return {"status": "not running"}
        self._backtests[backtest_id].terminate()
        self._backtests[backtest_id].join(timeout=5)
        return {"status": "stopped"}

    async def run_strategy(self, deployment_id: UUID) -> dict:
        if (
            deployment_id in self._deployments
            and self._deployments[deployment_id].is_alive()
        ):
            return {"status": "already running"}

        p = Process(target=_run_strategy, args=(deployment_id,))
        p.start()
        self._deployments[deployment_id] = p
        return {"status": "deployed"}

    async def stop_strategy(self, deployment_id: UUID) -> dict:
        if (
            deployment_id not in self._deployments
            or not self._deployments[deployment_id].is_alive()
        ):
            return {"status": "not running"}

        self._deployments[deployment_id].terminate()
        self._deployments[deployment_id].join(timeout=5)
        return {"status": "stopped"}

    async def stop_all(self) -> dict:
        for backtest_id, process in self._backtests.items():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        for deployment_id, process in self._deployments.items():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        return {"status": "all stopped"}
