import logging
from uuid import UUID

from core.db import get_db_session
from .channel import NotificationChannelType
from .enums import NotificationStatus, NotificationType
from .model import Notification as NotificationModel
from .schema import NotificationContextUnion


class NotificationPublisher:

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    async def publish(
        self,
        user_id: UUID,
        type: NotificationType,
        context: NotificationContextUnion,
        channel_type: NotificationChannelType = NotificationChannelType.EMAIL,
    ) -> None:
        notification = NotificationModel(
            user_id=user_id,
            type=type.value,
            context=context.model_dump(mode="json"),
            channel_type=channel_type.value,
            status=NotificationStatus.PENDING,
        )

        async with get_db_session() as db_sess:
            db_sess.add(notification)
            await db_sess.commit()

        self._logger.info(
            f"Enqueued notification '{notification.id}' of type '{type.value}' for user '{user_id}'"
        )
