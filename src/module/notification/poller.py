import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import case, select, update

from core.db import get_db_session
from .channel import NotificationChannel, NotificationChannelType
from .enums import NotificationStatus, NotificationType
from .model import Notification as NotificationModel
from .schema import (
    BacktestCapacityConstrainedNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
    Notification,
    NotificationContextUnion,
)


class NotificationPoller:

    def __init__(
        self,
        notification_channels: dict[NotificationChannelType, NotificationChannel],
        *,
        interval: int = 5,
        batch_size: int = 100,
        timeout: int = 30,
    ) -> None:
        self._notification_channels = notification_channels
        self.interval = interval
        self.batch_size = batch_size
        self.timeout = timeout
        self._logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        self._logger.info(
            "Starting notification poller (interval=%ss, batch_size=%s)",
            self.interval,
            self.batch_size,
        )

        while True:
            try:
                await asyncio.sleep(self.interval)

                records = await self._fetch_events()

                if not records:
                    continue

                self._logger.info("Processing %s notifications", len(records))

                results = await asyncio.gather(
                    *[self._emit_notification(record) for record in records],
                    return_exceptions=True,
                )

                updates: list[tuple[UUID, NotificationStatus]] = []
                success_count = 0
                failed_count = 0

                for result in results:
                    if isinstance(result, Exception):
                        self._logger.exception(
                            "Unhandled exception while processing notification batch",
                            exc_info=result,
                        )
                        failed_count += 1
                        continue

                    event_id, success = result
                    status = (
                        NotificationStatus.COMPLETED
                        if success
                        else NotificationStatus.FAILED
                    )
                    updates.append((event_id, status))

                    if success:
                        success_count += 1
                    else:
                        failed_count += 1

                if updates:
                    await self._update_events(updates)

                self._logger.info(
                    "Completed notification batch "
                    "(processed=%s, succeeded=%s, failed=%s)",
                    len(updates),
                    success_count,
                    failed_count,
                )

            except Exception as e:
                self._logger.exception(
                    "Unexpected error in notification poller loop", exc_info=e
                )

    async def _fetch_events(self) -> list[NotificationModel]:
        self._logger.info(
            "Fetching pending notifications (batch_size=%s)", self.batch_size
        )

        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(NotificationModel)
                .where(
                    NotificationModel.status.in_(
                        [
                            NotificationStatus.PENDING,
                            NotificationStatus.FAILED,
                        ]
                    )
                )
                .order_by(NotificationModel.created_at.asc())
                .limit(self.batch_size)
            )

            records = res.scalars().all()
            self._logger.info("Fetched %s notifications", len(records))
            return records

    async def _emit_notification(
        self, record: NotificationModel
    ) -> tuple[UUID, bool]:
        try:
            notification = self._build_notification(record)

            channel_type = NotificationChannelType(record.channel_type)
            channel = self._notification_channels.get(channel_type)
            if channel is None:
                self._logger.warning(
                    "No channel found for type '%s' (notification_id=%s)",
                    record.channel_type,
                    record.id,
                )
                return record.id, False

            await asyncio.wait_for(
                channel.send(notification), timeout=self.timeout
            )

            self._logger.info(
                "Successfully sent notification (id=%s, type=%s)",
                record.id,
                record.type,
            )

            return record.id, True

        except Exception:
            self._logger.warning(
                "Failed to send notification (id=%s, type=%s)",
                record.id,
                record.type,
                exc_info=True,
            )
            return record.id, False

    async def _update_events(
        self, updates: list[tuple[UUID, NotificationStatus]]
    ) -> None:
        if not updates:
            return

        self._logger.info("Updating %s notification statuses", len(updates))

        ids = [event_id for event_id, _ in updates]

        now = datetime.now(timezone.utc)
        stmt = (
            update(NotificationModel)
            .where(NotificationModel.id.in_(ids))
            .values(
                status=case(
                    *[
                        (NotificationModel.id == event_id, status.value)
                        for event_id, status in updates
                    ],
                    else_=NotificationModel.status,
                ),
                last_attempted_at=now,
            )
        )

        async with get_db_session() as db_sess:
            await db_sess.execute(stmt)
            await db_sess.commit()

        completed = sum(
            1 for _, status in updates if status == NotificationStatus.COMPLETED
        )
        failed = sum(
            1 for _, status in updates if status == NotificationStatus.FAILED
        )

        self._logger.info(
            "Updated notification statuses (completed=%s, failed=%s)",
            completed,
            failed,
        )

    def _build_notification(self, record: NotificationModel) -> Notification:
        context = self._parse_context(record.type, record.context)
        return Notification(
            user_id=record.user_id,
            type=NotificationType(record.type),
            context=context,
        )

    def _parse_context(
        self, notification_type: str, context_data: dict
    ) -> NotificationContextUnion:
        if notification_type == NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED:
            return DeploymentCapacityConstrainedNotificationContext.model_validate(
                context_data
            )
        if notification_type == NotificationType.BACKTEST_CAPACITY_CONSTRAINED:
            return BacktestCapacityConstrainedNotificationContext.model_validate(
                context_data
            )
        raise ValueError(f"Unknown notification type: {notification_type}")
