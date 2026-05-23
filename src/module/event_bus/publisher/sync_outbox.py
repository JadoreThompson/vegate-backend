from sqlalchemy import insert
from sqlalchemy.orm import Session

from module.event_bus.enums import EventStatus
from core.db import get_db_session
from core.event import BaseEvent
from .sync_publisher import SyncEventPublisher
from ..model import EventOutbox


class SyncOutboxEventPublisher(SyncEventPublisher):

    def __init__(self):
        super().__init__()

    def enqueue(self, event, db_sess=None):
        if db_sess is None:
            with get_db_session() as db_sess:
                self._persist_event(event, db_sess)
        else:
            self._persist_event(event, db_sess)

    def _persist_event(self, event: BaseEvent, db_sess: Session):
        db_sess.execute(
            insert(EventOutbox).values(
                type=event.type,
                payload=event.model_dump(mode="json"),
                timestamp=event.timestamp,
                status=EventStatus.PENDING,
            )
        )
        db_sess.commit()
