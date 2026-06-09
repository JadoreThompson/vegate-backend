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
from module.notification.publisher import NotificationPublisher
from module.notification.schema import DeploymentCapacityConstrainedNotificationContext
from module.notification.enums import NotificationType
from module.strategy.model import Strategy, StrategyVersion
from module.user.model import User
from .deserialiser import DeploymentEventDeserialiser
from .event import (
    DeploymentEventUnion,
    DeploymentCancelledEvent,
    DeploymentEventType,
    DeploymentRequestedEvent,
    DeploymentStatusChangedEvent,
    DeploymentStopRequestedEvent,
)
from ..enums import StrategyDeploymentStatus
from ..executor import DeploymentExecutor
from ..executor.exception import DeploymentLimitReached
from ..model import StrategyDeployments, DeploymentEvent


class DeploymentEventListenerService:
    """
    Consumes strategy deployment events, persists them to the DB,
    and monitors deployment heartbeats to transition statuses.
    """

    def __init__(
        self,
        *,
        deserialiser: DeploymentEventDeserialiser,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        deployment_executor: DeploymentExecutor,
        notification_publisher: NotificationPublisher,
        heartbeat_prefix_key: str = REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._deserialiser = deserialiser
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._deployment_executor = deployment_executor
        self._notification_publisher = notification_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self._monitor_interval = monitor_interval
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._pending_deployments: set[UUID] = set()
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
        res = await asyncio.gather(self._listen_loop(), self._monitor_loop())
        self._alive = False
        if res:
            raise ExceptionGroup("", res)

    async def _persist(self, event: DeploymentEventUnion) -> None:
        async with get_db_session() as session:
            deployment = await session.get(StrategyDeployments, event.deployment_id)
            if deployment is None:
                self._logger.info(
                    f"Deployment with id '{event.deployment_id}' not found."
                )
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
                deployment.status = event.status

            if event.type == DeploymentEventType.DEPLOYMENT_CANCELLED:
                deployment.status = StrategyDeploymentStatus.CANCELLED

            await session.commit()

    async def _handle_status_changed(self, event: DeploymentStatusChangedEvent) -> None:
        if event.status == StrategyDeploymentStatus.RUNNING:
            self._logger.info(
                f"Handling status changed. Pushing deployment with id '{event.deployment_id}' to running deployments"
            )
            async with self._lock:
                self._pending_deployments.discard(event.deployment_id)
                self._suspicious_deployments.discard(event.deployment_id)
                self._running_deployments.add(event.deployment_id)

    async def _handle_deployment_requested(
        self, event: DeploymentRequestedEvent
    ) -> None:
        async with get_db_session() as session:
            deployment = await session.get(StrategyDeployments, event.deployment_id)

        if deployment is None:
            self._logger.info(
                f"Deployment '{event.deployment_id}' not found, dropping event"
            )
            return
        if deployment.status not in {
            StrategyDeploymentStatus.PENDING,
            StrategyDeploymentStatus.STOPPED,
        }:
            self._logger.info(
                f"Dropping deployment requested event for '{event.deployment_id}' "
                f"with status '{deployment.status}'"
            )
            return

        if (
            deployment.deployment_id in self._pending_deployments
            or deployment.deployment_id in self._running_deployments
            or deployment.deployment_id in self._suspicious_deployments
        ):
            self._logger.info(
                f"Deployment '{event.deployment_id}' already running, suspicious or pending, dropping event"
            )
            return

        self._logger.info(f"Running deployment '{event.deployment_id}' via executor")
        try:
            await self._deployment_executor.run(event.deployment_id)
            async with self._lock:
                self._pending_deployments.add(event.deployment_id)
        except DeploymentLimitReached:
            self._logger.warning(
                f"Deployment limit reached, cannot run deployment '{event.deployment_id}'"
            )
            cancelled_event = DeploymentCancelledEvent(
                deployment_id=event.deployment_id, reason="capacity_constraint"
            )
            await self._event_publisher.publish(cancelled_event)

    async def _handle_deployment_stop_requested(
        self, event: DeploymentStopRequestedEvent
    ) -> None:
        async with get_db_session() as db_sess:
            deployment = await db_sess.get(StrategyDeployments, event.deployment_id)
            if deployment is None:
                self._logger.info(
                    f"Deployment '{event.deployment_id}' not found, dropping stop request"
                )
                return
            if deployment.status != StrategyDeploymentStatus.RUNNING:
                self._logger.info(
                    f"Deployment '{event.deployment_id}' is not running, ignoring stop request"
                )
                return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                self._logger.info(
                    f"Attempting to stop deployment '{event.deployment_id}'"
                )
                await self._deployment_executor.stop(event.deployment_id)
                self._logger.info(
                    f"Successfully sent stop request for deployment '{event.deployment_id}'"
                )
                return
            except Exception as e:
                self._logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed to stop "
                    f"deployment '{event.deployment_id}': {e}"
                )

            await asyncio.sleep(2**attempt)

        self._logger.error(
            f"Failed to stop deployment '{event.deployment_id}' after "
            f"{max_retries} attempts"
        )

    async def _handle_deployment_cancelled(
        self, event: DeploymentCancelledEvent
    ) -> None:
        user_id = await self._get_user_id_for_deployment(event.deployment_id)

        if event.reason == "capacity_constraint":
            context = DeploymentCapacityConstrainedNotificationContext(
                deployment_id=event.deployment_id
            )
            await self._notification_publisher.publish(
                user_id=user_id,
                type=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED,
                context=context,
            )
            async with get_db_session() as db_sess:
                await db_sess.execute(
                    update(StrategyDeployments)
                    .values(status=StrategyDeploymentStatus.CANCELLED)
                    .where(StrategyDeployments.deployment_id == event.deployment_id)
                )
                await db_sess.commit()
        else:
            raise ValueError(f"Unknown deployment cancellation reason '{event.reason}'")

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
                    await self._handle_status_changed(event)
                elif event.type == DeploymentEventType.DEPLOYMENT_REQUESTED:
                    await self._handle_deployment_requested(event)
                elif event.type == DeploymentEventType.DEPLOYMENT_STOP_REQUESTED:
                    await self._handle_deployment_stop_requested(event)
                elif event.type == DeploymentEventType.DEPLOYMENT_CANCELLED:
                    await self._handle_deployment_cancelled(event)

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
                    pending_deployments = self._pending_deployments.copy()
                    running_deployments = self._running_deployments.copy()
                    suspicious_deployments = self._suspicious_deployments.copy()

                self._logger.info(
                    f"Pending: {pending_deployments}, Running: {running_deployments}, "
                    f"Suspicious: {suspicious_deployments}"
                )

                deployment_ids = {
                    UUID(key.decode().replace(self._heartbeat_prefix_key, ""))
                    async for key in self._redis_client.scan_iter(
                        match=f"{self._heartbeat_prefix_key}*"
                    )
                }

                to_suspicious = []
                to_stopped = []
                to_running = []

                for pending_id in pending_deployments:
                    if pending_id not in deployment_ids:
                        to_suspicious.append(pending_id)
                        deployment_ids.discard(pending_id)

                for running_id in running_deployments:
                    if running_id not in deployment_ids:
                        to_suspicious.append(running_id)
                        deployment_ids.discard(running_id)

                for suspicious_id in suspicious_deployments:
                    if suspicious_id not in deployment_ids:
                        to_stopped.append(suspicious_id)
                        deployment_ids.discard(suspicious_id)

                for deployment_id in deployment_ids:
                    to_running.append(deployment_id)

                for id in to_suspicious:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=id,
                            status=StrategyDeploymentStatus.SUSPICIOUS,
                        )
                    )
                for id in to_stopped:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=id,
                            status=StrategyDeploymentStatus.STOPPED,
                        )
                    )
                for id in to_running:
                    await self._event_publisher.publish(
                        DeploymentStatusChangedEvent(
                            deployment_id=id,
                            status=StrategyDeploymentStatus.RUNNING,
                        )
                    )

                async with self._lock:
                    for item in to_suspicious:
                        self._pending_deployments.discard(item)
                        self._running_deployments.discard(item)
                        self._suspicious_deployments.add(item)
                    for item in to_stopped:
                        self._suspicious_deployments.discard(item)
                    for item in to_running:
                        self._pending_deployments.discard(item)
                        self._suspicious_deployments.discard(item)
                        self._running_deployments.add(item)

        except asyncio.CancelledError:
            pass

    async def _get_user_id_for_deployment(self, deployment_id: UUID) -> UUID:
        async with get_db_session() as session:
            user_id = await session.scalar(
                select(User.user_id)
                .select_from(StrategyDeployments)
                .join(
                    StrategyVersion,
                    StrategyVersion.id == StrategyDeployments.version_id,
                )
                .join(Strategy, Strategy.strategy_id == StrategyVersion.strategy_id)
                .join(User, User.user_id == Strategy.user_id)
                .where(StrategyDeployments.deployment_id == deployment_id)
            )

        if user_id is None:
            raise Exception(f"User not found for deployment '{deployment_id}'")
        return user_id
