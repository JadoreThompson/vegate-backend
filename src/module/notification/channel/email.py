from uuid import UUID

from sqlalchemy import select

from core.db import get_db_session
from module.email import EmailService
from module.user.model import User
from ..channel import NotificationChannel
from ..exception import NotificationException
from ..schema import Notification
from ..template.email import EmailNotificationTemplateEngine


class EmailNotificationChannel(NotificationChannel):

    def __init__(
        self,
        email_service: EmailService,
        template_engine: EmailNotificationTemplateEngine,
    ) -> None:
        self._email_service = email_service
        self._template_engine = template_engine

    async def send(self, notification: Notification) -> None:
        recipient = await self._resolve_recipient(notification.user_id)
        rendered = self._template_engine.render(notification, recipient)
        await self._email_service.send_email(
            recipient=recipient, subject=rendered.subject, body=rendered.body
        )

    async def _resolve_recipient(self, user_id: UUID) -> str:
        async with get_db_session() as db_sess:
            email = await db_sess.scalar(select(User.email).where(User.id == user_id))

        if email is None:
            raise NotificationException(f"No user found for user_id '{user_id}'")

        return email
