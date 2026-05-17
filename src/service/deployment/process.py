from multiprocessing import Process
from uuid import UUID

from config import OHLC_FEED_HOST, OHLC_FEED_PORT, OMS_BASE_URL
from infra.redis import REDIS_CLIENT_SYNC
from service.deployment.base import DeploymentService
from service.deployment.exception import DeploymentNotFoundException
from service.event.publisher import SyncEventPublisher
from service.ohlc.feed.client import OHLCFeedClient
from service.oms.client import OMSClient
from strategy.service import StrategyDeploymentService


def _run_strategy(deployment_id: UUID):
    ohlc_feed_client = OHLCFeedClient(host=OHLC_FEED_HOST, port=OHLC_FEED_PORT)
    oms_client = OMSClient(base_url=OMS_BASE_URL)
    event_publisher = SyncEventPublisher()

    sds = StrategyDeploymentService(
        deployment_id=deployment_id,
        ohlc_feed_client=ohlc_feed_client,
        oms_client=oms_client,
        event_publisher=event_publisher,
        redis_client=REDIS_CLIENT_SYNC
    )
    sds.setup()
    sds.run()


class ProcessDeploymentService(DeploymentService):

    def __init__(self):
        super().__init__()
        self._deployments: dict[UUID, Process] = {}

    async def run(self, deployment_id: UUID):
        if deployment_id in self._deployments:
            if self._deployments[deployment_id].is_alive():
                return

            self._deployments[deployment_id].kill()
            self._deployments[deployment_id].join(timeout=5)

        p = Process(target=_run_strategy, args=(deployment_id,))
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
