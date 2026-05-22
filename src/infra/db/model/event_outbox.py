import uuid
from datetime import datetime

from sqlalchemy import UUID, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from enums import EventStatus
from infra.db.model.base import Base, datetime_tz
from utils import get_datetime


class EventOutbox(Base):
    __tablename__ = "event_outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, primary_key=True
    )
    type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[EventStatus] = mapped_column(String, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
