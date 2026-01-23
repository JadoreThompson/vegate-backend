from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import ForeignKey, String, Text, UniqueConstraint, UUID as SaUUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infra.db.models.base import Base, datetime_tz, uuid_pk
from utils import get_datetime

if TYPE_CHECKING:
    from infra.db.models.users import Users
    from infra.db.models.backtests import Backtests
    from infra.db.models.strategy_deployments import StrategyDeployments


class Strategies(Base):
    __tablename__ = "strategies"
    __table_args__ = (UniqueConstraint("user_id", "name"),)

    strategy_id: Mapped[UUID] = uuid_pk()
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(nullable=False, onupdate=get_datetime)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Relationships
    backtests: Mapped[list["Backtests"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    strategy_deployments: Mapped[list["StrategyDeployments"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    user: Mapped["Users"] = relationship(back_populates="strategies")
