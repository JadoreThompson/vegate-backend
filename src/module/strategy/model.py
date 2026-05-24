from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String, Text, UUID as SaUUID
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, uuid_pk, datetime_tz
from util import get_datetime


class Strategy(Base):
    __tablename__ = "strategy"

    strategy_id: Mapped[UUID] = uuid_pk()
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(nullable=False, onupdate=get_datetime)
    prompt: Mapped[str] = mapped_column(Text, nullable=True)
    code: Mapped[str | None] = mapped_column(Text, nullable=True)
