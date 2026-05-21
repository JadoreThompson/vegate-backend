from sqlalchemy import insert
from sqlalchemy.orm import Session

from enums import EventStatus
from events.base import BaseEvent
from infra.db.model.event_outbox import EventOutbox
from infra.db.utils import get_db_session
from .sync_publisher import SyncEventPublisher


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
