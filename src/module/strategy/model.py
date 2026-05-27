import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, String, Text, UUID
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, uuid_pk, datetime_tz
from util import get_datetime


class Strategy(Base):
    __tablename__ = "strategy"

    strategy_id: Mapped[uuid.UUID] = uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.user_id", name="strategy_user_id_fkey", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(nullable=False, onupdate=get_datetime)
    cur_version_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "strategy_versions.id",
            name="strategy_cur_version_id_fkey",
            ondelete="SET NULL",
        ),
        nullable=True,
    )


class StrategyVersion(Base):
    __tablename__ = "strategy_versions"

    id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "strategy.strategy_id",
            name="strategy_versions_strategy_id_fkey",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    prev_version: Mapped[UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(
            "strategy_versions.id",
            name="strategy_versions_prev_version_fkey",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    code: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
