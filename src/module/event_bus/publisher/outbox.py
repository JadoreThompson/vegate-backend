from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from module.event_bus.enums import EventStatus
from core.event import BaseEvent
from core.db import get_db_session
from .base import EventPublisher
from ..model import EventOutbox


class OutboxEventPublisher(EventPublisher):

    def __init__(self):
        super().__init__()

    async def publish(self, event, db_sess=None):
        if db_sess is None:
            async with get_db_session() as db_sess:
                await self._persist_event(event, db_sess)
        else:
            await self._persist_event(event, db_sess)

    async def _persist_event(self, event: BaseEvent, db_sess: AsyncSession):
        await db_sess.execute(
            insert(EventOutbox).values(
                id=event.id,
                type=event.type,
                payload=event.model_dump(mode="json"),
                timestamp=event.timestamp,
                status=EventStatus.PENDING,
            )
        )
        await db_sess.commit()
