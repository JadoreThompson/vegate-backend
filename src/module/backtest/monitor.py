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
from .enums import BacktestStatus
from .event import BacktestEvent, BacktestEventType, BacktestStatusChangedEvent
from .event.deserialiser import BacktestEventDeserialiser
from .model import Backtest, BacktestEvent as BacktestEventModel


class BacktestMonitor:
    """
    Consumes backtest events, persists them to the DB,
    and monitors backtest heartbeats to transition statuses.
    """

    def __init__(
        self,
        deserialiser: BacktestEventDeserialiser,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        heartbeat_prefix_key: str = REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._deserialiser = deserialiser
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self._monitor_interval = monitor_interval
        self._kafka_consumer: AsyncKafkaConsumer | None = None
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
                select(
                    Backtest.id, Backtest.status
                ).where(
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
        res = await asyncio.gather(
            self._listen_loop(), self._monitor_loop(), return_exceptions=True
        )
        self._alive = False
        if res:
            raise ExceptionGroup("", res)

    async def _persist(self, event: BacktestEvent) -> None:
        async with get_db_session() as session:
            backtest = await session.get(Backtest, event.backtest_id)
            if backtest is None:
                self._logger.info(f"Backtest with id '{event.backtest_id}' not found.")
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

            await session.commit()

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
                    if event.status == BacktestStatus.IN_PROGRESS:
                        self._logger.info(
                            f"Pushing backtest with id '{event.backtest_id}' to running backtests"
                        )
                        async with self._lock:
                            self._running_backtests.add(event.backtest_id)
                    elif event.status in (
                        BacktestStatus.COMPLETED,
                        BacktestStatus.FAILED,
                        BacktestStatus.CANCELLED,
                    ):
                        self._logger.info(
                            f"Removing backtest with id '{event.backtest_id}' from watchlist"
                        )
                        async with self._lock:
                            self._running_backtests.discard(event.backtest_id)
                            self._suspicious_backtests.discard(event.backtest_id)

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
                    running_backtests = list(self._running_backtests)
                    suspicious_backtests = list(self._suspicious_backtests)

                if not running_backtests and not suspicious_backtests:
                    continue

                async with self._redis_client.pipeline() as pl:
                    for id in running_backtests:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")
                    for id in suspicious_backtests:
                        pl.get(f"{self._heartbeat_prefix_key}{id}")

                    results = await pl.execute()

                to_suspicious = []
                to_failed = []
                to_running = []
                for i in range(len(results)):
                    res = results[i]
                    self._logger.info(f"Result {i + 1} - {res}")
                    if i < len(running_backtests):
                        backtest_id = running_backtests[i]
                        if not res:
                            self._logger.info(
                                f"Pushing backtest '{backtest_id}' to suspicious"
                            )
                            to_suspicious.append(backtest_id)
                    elif i < len(suspicious_backtests):
                        backtest_id = suspicious_backtests[i]
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

                for id in to_suspicious:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.SUSPICIOUS
                    )
                    await self._event_publisher.enqueue(event)
                for id in to_failed:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.FAILED
                    )
                    await self._event_publisher.enqueue(event)
                for id in to_running:
                    event = BacktestStatusChangedEvent(
                        backtest_id=id, status=BacktestStatus.IN_PROGRESS
                    )
                    await self._event_publisher.enqueue(event)

                async with self._lock:
                    for item in to_suspicious:
                        self._running_backtests.discard(item)
                        self._suspicious_backtests.add(item)
                    for item in to_failed:
                        self._suspicious_backtests.discard(item)
                    for item in to_running:
                        self._suspicious_backtests.discard(item)
                        self._running_backtests.add(item)

        except asyncio.CancelledError:
            pass
