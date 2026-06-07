from sqlalchemy import insert
from sqlalchemy.orm import Session

from module.event_bus.enums import EventStatus
from core.db import get_db_sess_sync
from core.event import BaseEvent
from .base import SyncEventPublisher
from ..model import EventOutbox


class SyncOutboxEventPublisher(SyncEventPublisher):

    def __init__(self):
        super().__init__()

    def publish(self, event, db_sess=None):
        if db_sess is None:
            with get_db_sess_sync() as db_sess:
                self._persist_event(event, db_sess)
                db_sess.commit()
        else:
            self._persist_event(event, db_sess)

    def _persist_event(self, event: BaseEvent, db_sess: Session):
        db_sess.execute(
            insert(EventOutbox).values(
                id=event.id,
                type=event.type,
                payload=event.model_dump(mode="json"),
                timestamp=event.timestamp,
                status=EventStatus.PENDING,
            )
        )
