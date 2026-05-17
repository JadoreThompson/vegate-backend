from abc import abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession

from events.base import BaseEvent
from infra.db.model.event_outbox import EventOutbox


class EventPublisher:

    async def enqueue(
            self, event: BaseEvent, topic: str, db_sess: AsyncSession | None = None
    ): ...


class SyncEventPublisher:

    def enqueue(
            self, event: BaseEvent, topic: str, db_sess: AsyncSession | None = None
    ): ...
