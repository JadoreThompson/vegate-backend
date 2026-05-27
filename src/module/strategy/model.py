import uuid
from datetime import datetime

# from uuid import UUID

from sqlalchemy import ForeignKey, String, Text, UUID
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, uuid_pk, datetime_tz
from util import get_datetime


class Strategy(Base):
    __tablename__ = "strategy"

    strategy_id: Mapped[uuid.UUID] = uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(nullable=False, onupdate=get_datetime)
    prompt: Mapped[str] = mapped_column(Text, nullable=True)
    code: Mapped[str | None] = mapped_column(Text, nullable=True)


class StrategyVersion(Base):
    __tablename__ = "strategy_version"

    id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy.strategy_id", ondelete="CASCADE"),
        nullable=False,
    )
    prev_version: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_version.id", ondelete="SET NULL"),
        nullable=True,
    )
    code: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
