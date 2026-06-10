import asyncio
import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from core.db import get_db_session
from core.kafka import AsyncKafkaConsumer
from module.event_bus import EventPublisher
from module.notification.publisher import NotificationPublisher
from module.notification.schema import DeploymentCapacityConstrainedNotificationContext
from module.notification.enums import NotificationType
from module.strategy.model import Strategy, StrategyVersion
from module.user.model import User
from .state import State
from ..enums import StrategyDeploymentStatus
from ..event import (
    DeploymentEventUnion,
    DeploymentCancelledEvent,
    DeploymentEventType,
    DeploymentRequestedEvent,
    DeploymentStatusChangedEvent,
    DeploymentStopRequestedEvent,
)
from ..event.deserialiser import DeploymentEventDeserialiser
from ..executor import DeploymentExecutor
from ..executor.exception import DeploymentLimitReached
from ..model import StrategyDeployments, DeploymentEvent


class DeploymentEventHandler:

    def __init__(
        self,
        *,
        state: State,
        deserialiser: DeploymentEventDeserialiser,
        event_publisher: EventPublisher,
        deployment_executor: DeploymentExecutor,
        notification_publisher: NotificationPublisher,
    ):
        self._state = state
        self._deserialiser = deserialiser
        self._event_publisher = event_publisher
        self._deployment_executor = deployment_executor
        self._notification_publisher = notification_publisher
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def stop(self) -> None:
        if self._kafka_consumer:
            await self._kafka_consumer.stop()

    async def run(self) -> None:
        self._kafka_consumer = AsyncKafkaConsumer(
            STRATEGY_DEPLOYMENT_EVENTS_KEY,
            group_id="deployment_event_monitor_group",
            enable_auto_commit=False,
        )

        try:
            await self._kafka_consumer.start()

            async for record in self._kafka_consumer:
                event = self._deserialiser.deserialise_json(record.value)

                async with get_db_session() as db_sess:
                    deployment = await self._persist(event, db_sess)
                    if deployment is not None:
                        await self._handle(event, deployment, db_sess)
                    await db_sess.commit()

                await self._kafka_consumer.commit()
        finally:
            await self._kafka_consumer.stop()

    async def _persist(
        self, event: DeploymentEventUnion, db_sess: AsyncSession
    ) -> StrategyDeployments | None:
        deployment = await db_sess.get(StrategyDeployments, event.deployment_id)
        if deployment is None:
            self._logger.info(
                f"Deployment '{event.deployment_id}' not found, dropping event"
            )
            return None

        await db_sess.execute(
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

        return deployment

    async def _handle(
        self,
        event: DeploymentEventUnion,
        deployment: StrategyDeployments,
        db_sess: AsyncSession,
    ) -> None:
        if event.type == DeploymentEventType.DEPLOYMENT_STATUS:
            await self._handle_status_changed(event, deployment, db_sess)
        elif event.type == DeploymentEventType.DEPLOYMENT_REQUESTED:
            await self._handle_deployment_requested(event, deployment)
        elif event.type == DeploymentEventType.DEPLOYMENT_STOP_REQUESTED:
            await self._handle_deployment_stop_requested(event, deployment)
        elif event.type == DeploymentEventType.DEPLOYMENT_CANCELLED:
            await self._handle_deployment_cancelled(event, deployment, db_sess)

    async def _handle_status_changed(
        self,
        event: DeploymentStatusChangedEvent,
        deployment: StrategyDeployments,
        db_sess: AsyncSession,
    ) -> None:
        deployment.status = event.status

        if event.status == StrategyDeploymentStatus.RUNNING:
            self._logger.info(f"Deployment '{event.deployment_id}' is now running")
            await self._state.promote_to_running(event.deployment_id)

    async def _handle_deployment_requested(
        self,
        event: DeploymentRequestedEvent,
        deployment: StrategyDeployments,
    ) -> None:
        if deployment.status not in {
            StrategyDeploymentStatus.PENDING,
            StrategyDeploymentStatus.STOPPED,
        }:
            self._logger.info(
                f"Dropping deployment requested event for '{event.deployment_id}' "
                f"with status '{deployment.status}'"
            )
            return

        if await self._state.is_any(deployment.deployment_id):
            self._logger.info(
                f"Deployment '{event.deployment_id}' already tracked, dropping event"
            )
            return

        self._logger.info(f"Running deployment '{event.deployment_id}' via executor")
        try:
            await self._deployment_executor.run(event.deployment_id)
            await self._state.add_pending(event.deployment_id)
            await self._event_publisher.publish(
                DeploymentStatusChangedEvent(
                    deployment_id=event.deployment_id,
                    status=StrategyDeploymentStatus.PENDING,
                )
            )
        except DeploymentLimitReached:
            self._logger.warning(
                f"Deployment limit reached for '{event.deployment_id}'"
            )
            await self._event_publisher.publish(
                DeploymentCancelledEvent(
                    deployment_id=event.deployment_id, reason="capacity_constraint"
                )
            )

    async def _handle_deployment_stop_requested(
        self, event: DeploymentStopRequestedEvent, deployment: StrategyDeployments
    ) -> None:
        if not await self._state.is_any(event.deployment_id):
            self._logger.info(f"Deplyoment is not active. Dropping event '{event.id}'")
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                self._logger.info(
                    f"Attempting to stop deployment '{event.deployment_id}'"
                )
                await self._deployment_executor.stop(event.deployment_id)
                self._logger.info(
                    f"Stop request sent for deployment '{event.deployment_id}'"
                )
                return
            except Exception as e:
                self._logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed to stop "
                    f"deployment '{event.deployment_id}': {e}"
                )
            await asyncio.sleep(2**attempt)

        self._logger.error(
            f"Failed to stop deployment '{event.deployment_id}' after {max_retries} attempts"
        )

    async def _handle_deployment_cancelled(
        self,
        event: DeploymentCancelledEvent,
        deployment: StrategyDeployments,
        db_sess: AsyncSession,
    ) -> None:
        if event.reason == "capacity_constraint":
            user_id = await self._get_user_id_for_deployment(event.deployment_id)
            await self._notification_publisher.publish(
                user_id=user_id,
                type=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED,
                context=DeploymentCapacityConstrainedNotificationContext(
                    deployment_id=event.deployment_id
                ),
            )
            deployment.status = StrategyDeploymentStatus.CANCELLED
        else:
            raise ValueError(f"Unknown cancellation reason '{event.reason}'")

    async def _get_user_id_for_deployment(self, deployment_id: UUID) -> UUID:
        async with get_db_session() as session:
            user_id = await session.scalar(
                select(StrategyDeployments.user_id).where(
                    StrategyDeployments.deployment_id == deployment_id
                )
            )

        if user_id is None:
            raise Exception(f"User not found for deployment '{deployment_id}'")
        return user_id
