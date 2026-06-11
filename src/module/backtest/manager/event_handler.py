import asyncio
import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import BACKTEST_EVENTS_KEY
from core.db import get_db_session
from core.kafka import AsyncKafkaConsumer
from module.event_bus import EventPublisher
from module.notification.publisher import NotificationPublisher
from module.notification.schema import BacktestCapacityConstrainedNotificationContext
from module.notification.enums import NotificationType
from .state import BacktestState
from ..enums import BacktestStatus
from ..event import (
    BacktestCancelledEvent,
    BacktestEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
    BacktestStopRequestedEvent,
)
from ..event.deserialiser import BacktestEventDeserialiser
from ..executor import BacktestExecutor
from ..executor.exception import BacktestLimitReached
from ..model import Backtest, BacktestEvent as BacktestEventModel


class BacktestEventHandler:

    def __init__(
        self,
        *,
        state: BacktestState,
        deserialiser: BacktestEventDeserialiser,
        event_publisher: EventPublisher,
        backtest_executor: BacktestExecutor,
        notification_publisher: NotificationPublisher,
    ):
        self._state = state
        self._deserialiser = deserialiser
        self._event_publisher = event_publisher
        self._backtest_executor = backtest_executor
        self._notification_publisher = notification_publisher
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def stop(self) -> None:
        if self._kafka_consumer:
            await self._kafka_consumer.stop()

    async def run(self) -> None:
        self._kafka_consumer = AsyncKafkaConsumer(
            BACKTEST_EVENTS_KEY,
            group_id="backtest_event_monitor_group",
            enable_auto_commit=False,
        )

        try:
            await self._kafka_consumer.start()

            async for record in self._kafka_consumer:
                event = self._deserialiser.deserialise_json(record.value)

                async with get_db_session() as db_sess:
                    backtest = await self._persist(event, db_sess)
                    if backtest is not None:
                        await self._handle(event, backtest, db_sess)
                    await db_sess.commit()

                await self._kafka_consumer.commit()
        finally:
            await self._kafka_consumer.stop()

    async def _persist(
        self, event: BacktestEvent, db_sess: AsyncSession
    ) -> Backtest | None:
        backtest = await db_sess.get(Backtest, event.backtest_id)
        if backtest is None:
            self._logger.info(
                f"Backtest '{event.backtest_id}' not found, dropping event"
            )
            return None

        await db_sess.execute(
            insert(BacktestEventModel)
            .values(
                id=event.id,
                backtest_id=event.backtest_id,
                event_type=event.type,
                payload=event.model_dump(mode="json"),
                timestamp=event.timestamp,
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )

        return backtest

    async def _handle(
        self,
        event: BacktestEvent,
        backtest: Backtest,
        db_sess: AsyncSession,
    ) -> None:
        if event.type == BacktestEventType.STATUS_CHANGED:
            await self._handle_status_changed(event, backtest)
        elif event.type == BacktestEventType.REQUESTED:
            await self._handle_backtest_requested(event, backtest)
        elif event.type == BacktestEventType.STOP_REQUESTED:
            await self._handle_backtest_stop_requested(event, backtest)
        elif event.type == BacktestEventType.CANCELLED:
            await self._handle_backtest_cancelled(event, backtest, db_sess)
        else:
            self._logger.warning(f"Unknown event type received '{event.type}'")

    async def _handle_status_changed(
        self,
        event: BacktestStatusChangedEvent,
        backtest: Backtest,
    ) -> None:
        backtest.status = event.status

        if event.status == BacktestStatus.IN_PROGRESS:
            await self._state.promote_to_running(event.backtest_id)
            self._logger.info(f"Backtest '{event.backtest_id}' is now running")

        elif event.status in {BacktestStatus.FAILED, BacktestStatus.COMPLETED}:
            await self._state.discard(event.backtest_id)
            self._logger.info(f"Removing backtest '{event.backtest_id}' from monitor")

    async def _handle_backtest_requested(
        self,
        event: BacktestRequestedEvent,
        backtest: Backtest,
    ) -> None:
        if backtest.status not in {
            BacktestStatus.PENDING,
            BacktestStatus.COMPLETED,
            BacktestStatus.FAILED,
            BacktestStatus.CANCELLED,
        }:
            self._logger.info(
                f"Dropping backtest requested event for '{event.backtest_id}' "
                f"with status '{backtest.status}'"
            )
            return

        if await self._state.is_any(event.backtest_id):
            self._logger.info(
                f"Backtest '{event.backtest_id}' already tracked, dropping event"
            )
            return

        self._logger.info(f"Running backtest '{event.backtest_id}' via executor")
        try:
            await self._backtest_executor.run(event.backtest_id)
            await self._state.add_pending(event.backtest_id)
            await self._event_publisher.publish(
                BacktestStatusChangedEvent(
                    backtest_id=event.backtest_id, status=BacktestStatus.PENDING
                )
            )
        except BacktestLimitReached:
            self._logger.warning(
                f"Backtest limit reached, cannot run backtest '{event.backtest_id}'"
            )
            await self._event_publisher.publish(
                BacktestCancelledEvent(
                    backtest_id=event.backtest_id, reason="CAPACITY_CONSTRAINT"
                )
            )

    async def _handle_backtest_stop_requested(
        self,
        event: BacktestStopRequestedEvent,
        backtest: Backtest,
    ) -> None:
        if not await self._state.is_any(event.backtest_id):
            self._logger.info(f"Backtest is not active. Dropping event '{event.id}'")
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                self._logger.info(f"Attempting to stop backtest '{event.backtest_id}'")
                await self._backtest_executor.stop(event.backtest_id)
                self._logger.info(
                    f"Stop request sent for backtest '{event.backtest_id}'"
                )
                return
            except Exception as e:
                self._logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed to stop "
                    f"backtest '{event.backtest_id}': {e}"
                )
            await asyncio.sleep(2**attempt)

        self._logger.error(
            f"Failed to stop backtest '{event.backtest_id}' after {max_retries} attempts"
        )

    async def _handle_backtest_cancelled(
        self,
        event: BacktestCancelledEvent,
        backtest: Backtest,
        db_sess: AsyncSession,
    ) -> None:
        if event.reason == "CAPACITY_CONSTRAINT":
            user_id = await self._get_user_id_for_backtest(event.backtest_id)
            await self._notification_publisher.publish(
                user_id=user_id,
                type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
                context=BacktestCapacityConstrainedNotificationContext(
                    backtest_id=event.backtest_id
                ),
            )
            backtest.status = BacktestStatus.CANCELLED
        else:
            raise ValueError(f"Unknown cancellation reason '{event.reason}'")

    async def _get_user_id_for_backtest(self, backtest_id: UUID) -> UUID:
        async with get_db_session() as session:
            user_id = await session.scalar(
                select(Backtest.user_id).where(Backtest.id == backtest_id)
            )

        if user_id is None:
            raise Exception(f"User not found for backtest '{backtest_id}'")
        return user_id
