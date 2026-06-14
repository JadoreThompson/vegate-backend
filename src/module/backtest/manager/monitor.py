import asyncio
import logging
from uuid import UUID

from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import or_, select

from config import REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX
from core.db import get_db_sess_sync
from module.event_bus import EventPublisher
from .state import BacktestState
from ..enums import BacktestStatus
from ..event import BacktestStatusChangedEvent
from ..model import Backtest


class BacktestMonitor:

    def __init__(
        self,
        *,
        state: BacktestState,
        redis_client: AsyncRedis,
        event_publisher: EventPublisher,
        heartbeat_prefix_key: str = REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX,
        monitor_interval: int = 15,
    ):
        self._state = state
        self._redis_client = redis_client
        self._event_publisher = event_publisher
        self._heartbeat_prefix_key = heartbeat_prefix_key
        self.monitor_interval = monitor_interval
        self._logger = logging.getLogger(self.__class__.__name__)

    def setup(self) -> None:
        backtests = self._fetch_backtests()

        for backtest_id, status in backtests:
            if status == BacktestStatus.IN_PROGRESS:
                self._state._running.add(backtest_id)
            elif status == BacktestStatus.SUSPICIOUS:
                self._state._suspicious.add(backtest_id)
            else:
                self._state._pending.add(backtest_id)

    def _fetch_backtests(self) -> list[tuple[UUID, BacktestStatus]]:
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(Backtest.id, Backtest.status).where(
                    or_(
                        Backtest.status == BacktestStatus.IN_PROGRESS,
                        Backtest.status == BacktestStatus.SUSPICIOUS,
                        Backtest.status == BacktestStatus.PENDING,
                    )
                )
            )
            return res.all()

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
                to_failed = []
                to_running = []

                for bt_id in pending:
                    if bt_id not in heartbeat_ids:
                        to_suspicious.append(bt_id)
                        heartbeat_ids.discard(bt_id)

                for bt_id in running:
                    if bt_id not in heartbeat_ids:
                        to_suspicious.append(bt_id)
                        heartbeat_ids.discard(bt_id)

                for bt_id in suspicious:
                    if bt_id not in heartbeat_ids:
                        to_failed.append(bt_id)
                        heartbeat_ids.discard(bt_id)

                for bt_id in heartbeat_ids:
                    to_running.append(bt_id)

                for bt_id in to_suspicious:
                    await self._event_publisher.publish(
                        BacktestStatusChangedEvent(
                            backtest_id=bt_id, status=BacktestStatus.SUSPICIOUS
                        )
                    )
                for bt_id in to_failed:
                    await self._event_publisher.publish(
                        BacktestStatusChangedEvent(
                            backtest_id=bt_id, status=BacktestStatus.FAILED
                        )
                    )
                for bt_id in to_running:
                    await self._event_publisher.publish(
                        BacktestStatusChangedEvent(
                            backtest_id=bt_id, status=BacktestStatus.IN_PROGRESS
                        )
                    )

                for bt_id in to_suspicious:
                    await self._state.mark_suspicious(bt_id)
                for bt_id in to_failed:
                    await self._state.discard(bt_id)
                for bt_id in to_running:
                    await self._state.promote_to_running(bt_id)

        except asyncio.CancelledError:
            pass
