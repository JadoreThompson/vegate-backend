import asyncio
import logging
from uuid import UUID

from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import or_, select

from config import REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX
from core.db import get_db_sess_sync
from module.event_bus import EventPublisher
from .state import State
from ..enums import StrategyDeploymentStatus
from ..event import DeploymentStatusChangedEvent
from ..model import StrategyDeployments



class DeploymentMonitor:

    def __init__(
        self,
        *,
        state: State,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        heartbeat_prefix_key: str = REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._state = state
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self.monitor_interval = monitor_interval
        self._logger = logging.getLogger(self.__class__.__name__)

    def setup(self) -> None:
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(
                    StrategyDeployments.deployment_id, StrategyDeployments.status
                ).where(
                    or_(
                        StrategyDeployments.status == StrategyDeploymentStatus.RUNNING,
                        StrategyDeployments.status == StrategyDeploymentStatus.SUSPICIOUS,
                    )
                )
            )
            data = res.all()

        for deployment_id, status in data:
            if status == StrategyDeploymentStatus.RUNNING:
                self._state._running.add(deployment_id)
            else:
                self._state._suspicious.add(deployment_id)

    async def run(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.monitor_interval)

                pending, running, suspicious = await self._state.snapshot()

                self._logger.info(
                    f"Pending: {pending}, Running: {running}, Suspicious: {suspicious}"
                )

                heartbeat_ids = {
                    UUID(key.decode().replace(self._heartbeat_prefix_key, ""))
                    async for key in self._redis_client.scan_iter(
                        match=f"{self._heartbeat_prefix_key}*"
                    )
                }

                to_suspicious = []
                to_stopped = []
                to_running = []

                for deployment_id in pending:
                    if deployment_id not in heartbeat_ids:
                        to_suspicious.append(deployment_id)
                        heartbeat_ids.discard(deployment_id)

                for deployment_id in running:
                    if deployment_id not in heartbeat_ids:
                        to_suspicious.append(deployment_id)
                        heartbeat_ids.discard(deployment_id)

                for deployment_id in suspicious:
                    if deployment_id not in heartbeat_ids:
                        to_stopped.append(deployment_id)
                        heartbeat_ids.discard(deployment_id)

                # anything left in heartbeat_ids is an untracked but live deployment
                for deployment_id in heartbeat_ids:
                    to_running.append(deployment_id)
                
                print("to running:", to_running)
                print("to suspicious:", to_suspicious)

                for deployment_id in to_suspicious:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=deployment_id,
                            status=StrategyDeploymentStatus.SUSPICIOUS,
                        )
                    )
                for deployment_id in to_stopped:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=deployment_id,
                            status=StrategyDeploymentStatus.STOPPED,
                        )
                    )
                for deployment_id in to_running:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=deployment_id,
                            status=StrategyDeploymentStatus.RUNNING,
                        )
                    )

                for deployment_id in to_suspicious:
                    await self._state.mark_suspicious(deployment_id)
                for deployment_id in to_stopped:
                    await self._state.discard(deployment_id)
                for deployment_id in to_running:
                    await self._state.promote_to_running(deployment_id)

        except asyncio.CancelledError:
            pass