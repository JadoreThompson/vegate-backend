from abc import ABC, abstractmethod

from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from core.event import BaseEvent


class EventPublisher(ABC):

    @abstractmethod
    async def publish(self, event: BaseEvent, db_sess: AsyncSession | None = None) -> None: ...


class SyncEventPublisher(ABC):

    @abstractmethod
    def publish(self, event: BaseEvent, db_sess: Session | None = None): ...
