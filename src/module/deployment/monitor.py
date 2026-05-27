import asyncio
import logging
from uuid import UUID

from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert

from config import (
    REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX,
    STRATEGY_DEPLOYMENT_EVENTS_KEY,
)
from core.db import get_db_session, get_db_sess_sync
from core.kafka import AsyncKafkaConsumer
from module.event_bus import EventPublisher
from .enums import StrategyDeploymentStatus
from .event import BaseDeploymentEvent, DeploymentStatusChangedEvent, DeploymentEventType
from .event.deserialiser import DeploymentEventDeserialiser
from .model import StrategyDeployments, DeploymentEvent


class DeploymentEventMonitorService:
    """
    Consumes strategy deployment events, persists them to the DB,
    and monitors deployment heartbeats to transition statuses.
    """

    def __init__(
        self,
        deserialiser: DeploymentEventDeserialiser,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        heartbeat_prefix_key: str = REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._deserialiser = deserialiser
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self._monitor_interval = monitor_interval
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._running_deployments: set[UUID] = set()
        self._suspicious_deployments: set[UUID] = set()
        self._alive = False
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def monitor_interval(self):
        return self._monitor_interval

    async def stop(self):
        if self._kafka_consumer:
            await self._kafka_consumer.stop()

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

    async def _persist(self, event: BaseDeploymentEvent) -> None:
        async with get_db_session() as session:
            deployment = await session.get(StrategyDeployments, event.deployment_id)
            if deployment is None:
                self._logger.info(f"Deployment with id '{event.deployment_id}' not found.")
                return

            await session.execute(
                insert(DeploymentEvent)
                .values(
                    id=event.id,
                    deployment_id=event.deployment_id,
                    event_type=event.type,
                    payload=event.model_dump(mode="json"),
                    timestamp=event.timestamp,
                )
                .on_conflict_do_nothing(index_elements=["id"])
            )

            if event.type == DeploymentEventType.DEPLOYMENT_STATUS:
                await session.execute(
                    update(StrategyDeployments)
                    .where(StrategyDeployments.deployment_id == event.deployment_id)
                    .values(status=event.status)
                )

            await session.commit()

    async def _listen_loop(self):
        self._kafka_consumer = AsyncKafkaConsumer(
            STRATEGY_DEPLOYMENT_EVENTS_KEY,
            group_id="deployment_event_monitor_group",
            enable_auto_commit=False,
        )

        try:
            await self._kafka_consumer.start()

            async for record in self._kafka_consumer:
                event = self._deserialiser.deserialise_json(record.value)
                await self._persist(event)

                if event.type == DeploymentEventType.DEPLOYMENT_STATUS:
                    if event.status == StrategyDeploymentStatus.RUNNING:
                        self._logger.info(
                            f"Pushing deployment with id '{event.deployment_id}' to running deployments"
                        )
                        async with self._lock:
                            self._running_deployments.add(event.deployment_id)

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
                self._logger.info(f"Running deployments: {running_deployments}")
                self._logger.info(f"Suspicious deployments: {suspicious_deployments}")
                if not running_deployments and not suspicious_deployments:
                    continue

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
                            self._logger.info(
                                f"Pushing deployment '{deployment_id}' to suspicious"
                            )
                            to_suspicious.append(deployment_id)
                    else:
                        deployment_id = suspicious_deployments[i - len(running_deployments)]
                        if not res:
                            self._logger.info(
                                f"Pushing deployment '{deployment_id}' to stopped"
                            )
                            to_stopped.append(deployment_id)
                        else:
                            self._logger.info(
                                f"Pushing deployment '{deployment_id}' to running"
                            )
                            to_running.append(deployment_id)

                for id in to_suspicious:
                    event = DeploymentStatusChangedEvent(
                        deployment_id=id, status=StrategyDeploymentStatus.SUSPICIOUS
                    )
                    await self._event_publisher.enqueue(event)
                for id in to_stopped:
                    event = DeploymentStatusChangedEvent(
                        deployment_id=id, status=StrategyDeploymentStatus.STOPPED
                    )
                    await self._event_publisher.enqueue(event)
                for id in to_running:
                    event = DeploymentStatusChangedEvent(
                        deployment_id=id, status=StrategyDeploymentStatus.RUNNING
                    )
                    await self._event_publisher.enqueue(event)

                async with self._lock:
                    for item in to_suspicious:
                        self._running_deployments.discard(item)
                        self._suspicious_deployments.add(item)
                    for item in to_stopped:
                        self._suspicious_deployments.discard(item)
                    for item in to_running:
                        self._suspicious_deployments.discard(item)
                        self._running_deployments.add(item)

        except asyncio.CancelledError:
            pass
