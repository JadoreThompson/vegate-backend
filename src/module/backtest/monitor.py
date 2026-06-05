import asyncio
import logging
from uuid import UUID

from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert

from config import (
    REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX,
    BACKTEST_EVENTS_KEY,
)
from core.db import get_db_session, get_db_sess_sync
from core.kafka import AsyncKafkaConsumer
from module.event_bus import EventPublisher
from module.notification.publisher import NotificationPublisher
from module.notification.schema import BacktestCapacityConstrainedNotificationContext
from module.notification.enums import NotificationType
from module.strategy.model import Strategy, StrategyVersion
from module.user.model import User
from .event.deserialiser import BacktestEventDeserialiser
from .enums import BacktestStatus
from .event import (
    BacktestCancelledEvent,
    BacktestEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
    BacktestStopRequestedEvent,
)
from .executor import BacktestExecutor
from .executor.exception import BacktestLimitReached
from .model import Backtest, BacktestEvent as BacktestEventModel


class BacktestMonitor:
    """
    Consumes backtest events, persists them to the DB,
    monitors backtest heartbeats to transition statuses,
    and manages the backtest lifecycle (start, stop, cancel).
    """

    def __init__(
        self,
        deserialiser: BacktestEventDeserialiser,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        backtest_executor: BacktestExecutor,
        notification_publisher: NotificationPublisher,
        heartbeat_prefix_key: str = REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._deserialiser = deserialiser
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._backtest_executor = backtest_executor
        self._notification_publisher = notification_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self._monitor_interval = monitor_interval
        self._kafka_consumer: AsyncKafkaConsumer | None = None
        self._pending_backtests: set[UUID] = set()
        self._running_backtests: set[UUID] = set()
        self._suspicious_backtests: set[UUID] = set()
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
                select(Backtest.id, Backtest.status).where(
                    or_(
                        Backtest.status == BacktestStatus.IN_PROGRESS,
                        Backtest.status == BacktestStatus.SUSPICIOUS,
                    )
                )
            )
            data = res.all()
        for id, status in data:
            if status == BacktestStatus.IN_PROGRESS:
                self._running_backtests.add(id)
            else:
                self._suspicious_backtests.add(id)

    async def run(self):
        self._alive = True
        res = await asyncio.gather(self._listen_loop(), self._monitor_loop())
        self._alive = False
        if res:
            raise ExceptionGroup("", res)

    async def _persist(self, event: BacktestEvent) -> None:
        async with get_db_session() as session:
            backtest = await session.get(Backtest, event.backtest_id)
            if backtest is None:
                self._logger.info(
                    f"Backtest with id '{event.backtest_id}' not found."
                )
                return

            await session.execute(
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

            if event.type == BacktestEventType.STATUS_CHANGED:
                await session.execute(
                    update(Backtest)
                    .where(Backtest.id == event.backtest_id)
                    .values(status=event.status)
                )

            if event.type == BacktestEventType.CANCELLED:
                await session.execute(
                    update(Backtest)
                    .where(Backtest.id == event.backtest_id)
                    .values(status=BacktestStatus.CANCELLED)
                )

            await session.commit()

    async def _handle_status_changed(self, event: BacktestStatusChangedEvent) -> None:
        if event.status != BacktestStatus.IN_PROGRESS:
            return

        self._logger.info(
            f"Pushing backtest with id '{event.backtest_id}' to running backtests"
        )
        async with self._lock:
            self._pending_backtests.discard(event.backtest_id)
            self._suspicious_backtests.discard(event.backtest_id)
            self._running_backtests.add(event.backtest_id)

    async def _handle_backtest_requested(self, event: BacktestRequestedEvent) -> None:
        async with get_db_session() as session:
            backtest = await session.get(Backtest, event.backtest_id)

        if backtest is None:
            self._logger.info(
                f"Backtest '{event.backtest_id}' not found, dropping event"
            )
            return
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

        if (
            backtest.id in self._pending_backtests
            or backtest.id in self._running_backtests
            or backtest.id in self._suspicious_backtests
        ):
            self._logger.info(
                f"Backtest '{event.backtest_id}' already running, suspicious or pending, dropping event"
            )
            return

        self._logger.info(
            f"Running backtest '{event.backtest_id}' via executor"
        )
        try:
            await self._backtest_executor.run(event.backtest_id)
            async with self._lock:
                self._pending_backtests.add(event.backtest_id)
        except BacktestLimitReached:
            self._logger.warning(
                f"Backtest limit reached, cannot run backtest '{event.backtest_id}'"
            )
            cancelled_event = BacktestCancelledEvent(
                backtest_id=event.backtest_id, reason="CAPACITY_CONSTRAINT"
            )
            await self._event_publisher.publish(cancelled_event)

    async def _handle_backtest_stop_requested(
        self, event: BacktestStopRequestedEvent
    ) -> None:
        async with get_db_session() as session:
            backtest = await session.get(Backtest, event.backtest_id)
            if backtest is None:
                self._logger.info(
                    f"Backtest '{event.backtest_id}' not found, dropping stop request"
                )
                return
            if backtest.status != BacktestStatus.IN_PROGRESS:
                self._logger.info(
                    f"Backtest '{event.backtest_id}' is not running, ignoring stop request"
                )
                return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                await self._backtest_executor.stop(event.backtest_id)
                self._logger.info(
                    f"Successfully sent stop request for backtest '{event.backtest_id}'"
                )
                return
            except Exception as e:
                self._logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} failed to stop "
                    f"backtest '{event.backtest_id}': {e}"
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)

        self._logger.error(
            f"Failed to stop backtest '{event.backtest_id}' after "
            f"{max_retries} attempts"
        )

    async def _handle_backtest_cancelled(self, event: BacktestCancelledEvent) -> None:
        user_id = await self._get_user_id_for_backtest(event.backtest_id)
        context = BacktestCapacityConstrainedNotificationContext(
            backtest_id=event.backtest_id
        )
        await self._notification_publisher.publish(
            user_id=user_id,
            type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
            context=context,
        )

    async def _listen_loop(self):
        self._kafka_consumer = AsyncKafkaConsumer(
            BACKTEST_EVENTS_KEY,
            group_id="backtest_event_monitor_group",
            enable_auto_commit=False,
        )
        try:
            await self._kafka_consumer.start()

            async for record in self._kafka_consumer:
                event = self._deserialiser.deserialise_json(record.value)
                await self._persist(event)

                if event.type == BacktestEventType.STATUS_CHANGED:
                    await self._handle_status_changed(event)
                elif event.type == BacktestEventType.REQUESTED:
                    await self._handle_backtest_requested(event)
                elif event.type == BacktestEventType.STOP_REQUESTED:
                    await self._handle_backtest_stop_requested(event)
                elif event.type == BacktestEventType.CANCELLED:
                    await self._handle_backtest_cancelled(event)

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
                    pending_backtests = list(self._pending_backtests)
                    running_backtests = list(self._running_backtests)
                    suspicious_backtests = list(self._suspicious_backtests)
                self._logger.info(
                    f"Pending: {pending_backtests}, Running: {running_backtests}, "
                    f"Suspicious: {suspicious_backtests}"
                )
                if (
                    not pending_backtests
                    and not running_backtests
                    and not suspicious_backtests
                ):
                    continue

                async with self._redis_client.pipeline() as pl:
                    for id in running_backtests:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")
                    for id in suspicious_backtests:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")
                    for id in pending_backtests:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")

                    results = await pl.execute()

                to_suspicious = []
                to_failed = []
                to_running = []
                n_running = len(running_backtests)
                n_suspicious = len(suspicious_backtests)

                for i, res in enumerate(results):
                    self._logger.info(f"Result {i + 1} - {res}")

                    if i < n_running:
                        backtest_id = running_backtests[i]
                        if not res:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to suspicious"
                            )
                            to_suspicious.append(backtest_id)

                    elif i < n_running + n_suspicious:
                        backtest_id = suspicious_backtests[i - n_running]
                        if not res:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to failed"
                            )
                            to_failed.append(backtest_id)
                        else:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to running"
                            )
                            to_running.append(backtest_id)

                    else:
                        backtest_id = pending_backtests[
                            i - n_running - n_suspicious
                        ]
                        if res:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to running"
                            )
                            to_running.append(backtest_id)
                        else:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to suspicious"
                            )
                            to_suspicious.append(backtest_id)

                for id in to_suspicious:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.SUSPICIOUS
                    )
                    await self._event_publisher.publish(event)
                for id in to_failed:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.FAILED
                    )
                    await self._event_publisher.publish(event)
                for id in to_running:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.IN_PROGRESS
                    )
                    await self._event_publisher.publish(event)

                async with self._lock:
                    for item in to_suspicious:
                        self._pending_backtests.discard(item)
                        self._running_backtests.discard(item)
                        self._suspicious_backtests.add(item)
                    for item in to_failed:
                        self._suspicious_backtests.discard(item)
                    for item in to_running:
                        self._pending_backtests.discard(item)
                        self._suspicious_backtests.discard(item)
                        self._running_backtests.add(item)

        except asyncio.CancelledError:
            pass

    async def _get_user_id_for_backtest(self, backtest_id: UUID) -> UUID:
        async with get_db_session() as session:
            user_id = await session.scalar(
                select(User.user_id)
                .select_from(Backtest)
                .join(
                    StrategyVersion,
                    StrategyVersion.id == Backtest.version_id,
                )
                .join(Strategy, Strategy.strategy_id == StrategyVersion.strategy_id)
                .join(User, User.user_id == Strategy.user_id)
                .where(Backtest.id == backtest_id)
            )

        if user_id is None:
            raise Exception(f"User not found for backtest '{backtest_id}'")
        return user_id
