from datetime import datetime
import uuid
from sqlalchemy import UUID, String
from sqlalchemy.orm import Mapped, mapped_column

from enums import EventStatus
from infra.db.model.base import Base, datetime_tz, uuid_pk


class EventOutbox(Base):
    __tablename__ = "event_outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, primary_key=True
    )
    type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[EventStatus] = mapped_column(String, nullable=False)
    topic: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()
