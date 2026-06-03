import logging
from uuid import UUID

from .channel import NotificationChannel, NotificationChannelType
from .exception import NotificationException
from .schema import Notification, NotificationContextUnion, NotificationType


class NotificationPublisher:

    def __init__(
        self,
        notification_channels: dict[NotificationChannelType, NotificationChannel],
    ) -> None:
        self._notification_channels = notification_channels
        self._logger = logging.getLogger(self.__class__.__name__)

    async def publish(
        self,
        user_id: UUID,
        type: NotificationType,
        context: NotificationContextUnion,
        channel_type: NotificationChannelType = NotificationChannelType.EMAIL,
    ) -> None:
        notification = Notification(user_id=user_id, type=type, context=context)

        channel = self._notification_channels.get(channel_type)
        if not channel:
            raise NotificationException(f"No channel found for type {channel_type}")

        await channel.send(notification)
