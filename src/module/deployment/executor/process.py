from multiprocessing import Process
from uuid import UUID

from config import OHLC_FEED_HOST, OHLC_FEED_PORT, OMS_BASE_URL
from core.redis import REDIS_CLIENT_SYNC
from module.event_bus import EventPublisher, SyncEventPublisher
from module.markets.feed import OHLCFeedClient
from .base import DeploymentExecutor
from ..exception import DeploymentNotFoundException
from ..runner import StrategyDeploymentRunner
from ..oms import OMSClient


def _run_strategy_deployment(deployment_id: UUID):
    ohlc_feed_client = OHLCFeedClient(host=OHLC_FEED_HOST, port=OHLC_FEED_PORT)
    oms_client = OMSClient(base_url=OMS_BASE_URL)
    event_publisher = SyncEventPublisher()

    runner = StrategyDeploymentRunner(
        deployment_id=deployment_id,
        ohlc_feed_client=ohlc_feed_client,
        oms_client=oms_client,
        event_publisher=event_publisher,
        redis_client=REDIS_CLIENT_SYNC,
    )
    runner.run()


class ProcessDeploymentExecutor(DeploymentExecutor):

    def __init__(self):
        super().__init__()
        self._deployments: dict[UUID, Process] = {}
        self._event_publisher: EventPublisher | None = None

    def _get_event_publisher(self):
        if self._event_publisher is None:
            self._event_publisher = EventPublisher()
        return self._event_publisher

    async def run(self, deployment_id: UUID):
        if deployment_id in self._deployments:
            if self._deployments[deployment_id].is_alive():
                return

            self._deployments[deployment_id].kill()
            self._deployments[deployment_id].join(timeout=5)

        p = Process(target=_run_strategy_deployment, args=(deployment_id,))
        p.start()
        self._deployments[deployment_id] = p

    async def stop(self, deployment_id: UUID):
        if deployment_id not in self._deployments:
            raise DeploymentNotFoundException(deployment_id)

        self._deployments[deployment_id].terminate()
        self._deployments[deployment_id].join(timeout=5)
        self._deployments.pop(deployment_id)

    async def stop_all(self):
        for deployment_id, process in self._deployments.items():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

        self._deployments.clear()
