import uuid
from datetime import datetime

from sqlalchemy import UUID, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, datetime_tz
from .enums import NotificationStatus
from util import get_datetime, get_uuid


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=get_uuid
    )
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    context: Mapped[dict] = mapped_column(JSONB, nullable=False)
    channel_type: Mapped[str] = mapped_column(String, nullable=False, default="email")
    status: Mapped[NotificationStatus] = mapped_column(
        String, nullable=False, default=NotificationStatus.PENDING
    )
    last_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
