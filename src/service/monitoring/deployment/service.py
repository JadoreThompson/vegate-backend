import asyncio
import json
import logging
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import REDIS_STRATEGY_HEARTBEAT_KEY_PREFIX, STRATEGY_DEPLOYMENT_EVENTS_KEY
from enums import StrategyDeploymentStatus
from events.deployment import DeploymentStatusChangedEvent, DeploymentEventType
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.utils import get_db_sess_sync, get_db_session
from infra.kafka.client import AsyncKafkaConsumer
from service.event.publisher import EventPublisher


class DeploymentMonitoringService:

    def __init__(
        self,
        redis_client: Redis,
        event_publisher: EventPublisher,
        heartbeat_prefix_key: str = REDIS_STRATEGY_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._redis_client = redis_client
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._event_publisher = event_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self._monitor_interval = monitor_interval
        self._running_deployments: set[UUID] = set()
        self._suspicious_deployments: set[UUID] = set()
        self._alive = False
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def monitor_interval(self):
        return self._monitor_interval

    def setup(self):
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(
                    StrategyDeployments.deployment_id, StrategyDeployments.status
                ).where(
                    or_(
                        StrategyDeployments.status == StrategyDeploymentStatus.RUNNING,
                        StrategyDeployments.status
                        == StrategyDeploymentStatus.SUSPICIOUS,
                    )
                )
            )

            data = res.all()

        for id, status in data:
            if status == StrategyDeploymentStatus.RUNNING:
                self._running_deployments.add(id)
            else:
                self._suspicious_deployments.add(id)

    async def run(self):
        self._alive = True
        res = await asyncio.gather(
            self._listen_loop(), self._monitor_loop(), return_exceptions=True
        )
        self._alive = False
        if res:
            raise ExceptionGroup("", res)

    async def _listen_loop(self):
        self._kafka_consumer = AsyncKafkaConsumer(
            STRATEGY_DEPLOYMENT_EVENTS_KEY,
            group_id="deployment_monitoring_group",
            enable_auto_commit=False,
        )

        try:
            await self._kafka_consumer.start()

            async for record in self._kafka_consumer:
                for key, value in record.headers:
                    if (
                        key == "event_type"
                        and value.decode() == DeploymentEventType.DEPLOYMENT_STATUS
                    ):
                        event_data = json.loads(record.value)
                        if event_data["status"] == StrategyDeploymentStatus.RUNNING:
                            try:
                                deployment_id = UUID(event_data["deployment_id"])
                            except ValueError:
                                pass
                            self._logger.info(
                                f"Pushing deployment with id '{deployment_id}' to running deployments"
                            )
                            async with self._lock:
                                self._running_deployments.add(deployment_id)

                await self._kafka_consumer.commit()
        finally:
            await self._kafka_consumer.stop()

    async def _monitor_loop(self):
        try:
            while self._alive:
                await asyncio.sleep(self._monitor_interval)
                
                if not self._alive:
                    break
                
                async with self._lock:
                    running_deployments = list(self._running_deployments)
                    suspicious_deployments = list(self._suspicious_deployments)
                
                async with self._redis_client.pipeline() as pl:
                    for id in running_deployments:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")
                    for id in suspicious_deployments:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")
                    
                    results = await pl.execute()

                to_suspicious = []
                to_stopped = []
                to_running = []
                for i in range(len(results)):
                    res = results[i]
                    self._logger.info(f"Result {i + 1} - {res}")
                    if i < len(running_deployments):
                        deployment_id = running_deployments[i]

                        if not res:
                            self._logger.info(f"Pushing deployment '{deployment_id}' to suspicious")
                            to_suspicious.append(deployment_id)
                    
                    elif i < len(suspicious_deployments):
                        deployment_id = suspicious_deployments[i]

                        if not res:
                            self._logger.info(f"Pushing deployment '{deployment_id}' to stopped")
                            to_stopped.append(deployment_id)
                        else:
                            self._logger.info(f"Pushing deployment '{deployment_id}' to running")
                            to_running.append(deployment_id)
                
                async with get_db_session() as db_sess:
                    if to_suspicious:
                        await self._set_status_suspicious(to_suspicious, db_sess)
                    if to_stopped:
                        await self._set_status_stopped(to_stopped, db_sess)
                    if to_running:
                        await self._set_status_running(to_running, db_sess)
                    await db_sess.commit()

                async with self._lock:
                    for item in to_suspicious:
                        self._running_deployments.discard(item)
                        self._suspicious_deployments.add(item)
                    for item in to_stopped:
                        self._suspicious_deployments.discard(item)
                    for item in to_running:
                        self._suspicious_deployments.remove(item)
                        self._running_deployments.add(item)

        except asyncio.CancelledError:
            pass

    async def _set_status_suspicious(
        self, deployment_ids: list[UUID], db_sess: AsyncSession
    ):
        res = await db_sess.execute(
            update(StrategyDeployments)
            .where(
                StrategyDeployments.deployment_id.in_(deployment_ids),
                StrategyDeployments.status == StrategyDeploymentStatus.RUNNING,
            )
            .values(status=StrategyDeploymentStatus.SUSPICIOUS)
        )

        for id in deployment_ids:
            event = DeploymentStatusChangedEvent(
                deployment_id=id, status=StrategyDeploymentStatus.SUSPICIOUS
            )
            await self._event_publisher.enqueue(
                event, STRATEGY_DEPLOYMENT_EVENTS_KEY, db_sess
            )

    async def _set_status_stopped(
        self, deployment_ids: list[UUID], db_sess: AsyncSession
    ):
        await db_sess.execute(
            update(StrategyDeployments)
            .where(
                StrategyDeployments.deployment_id.in_(deployment_ids),
                StrategyDeployments.status == StrategyDeploymentStatus.SUSPICIOUS,
            )
            .values(status=StrategyDeploymentStatus.STOPPED)
        )

        for id in deployment_ids:
            event = DeploymentStatusChangedEvent(
                deployment_id=id, status=StrategyDeploymentStatus.STOPPED
            )
            await self._event_publisher.enqueue(
                event, STRATEGY_DEPLOYMENT_EVENTS_KEY, db_sess
            )

    async def _set_status_running(
        self, deployment_ids: list[UUID], db_sess: AsyncSession
    ):
        await db_sess.execute(
            update(StrategyDeployments)
            .where(
                StrategyDeployments.deployment_id.in_(deployment_ids),
                StrategyDeployments.status != StrategyDeploymentStatus.RUNNING,
            )
            .values(status=StrategyDeploymentStatus.RUNNING)
        )

        for id in deployment_ids:
            event = DeploymentStatusChangedEvent(
                deployment_id=id, status=StrategyDeploymentStatus.RUNNING
            )
            await self._event_publisher.enqueue(
                event, STRATEGY_DEPLOYMENT_EVENTS_KEY, db_sess
            )
