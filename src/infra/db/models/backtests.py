from datetime import date, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import Date, ForeignKey, String, UUID as SaUUID, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from engine.enums import Timeframe
from enums import BacktestStatus
from infra.db.models.base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from infra.db.models.strategies import Strategies
    from infra.db.models.orders import Orders


class Backtests(Base):
    __tablename__ = "backtests"

    backtest_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("strategies.strategy_id"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    starting_balance: Mapped[float] = mapped_column(
        Float, nullable=False
    )
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    start_date: Mapped[date] = mapped_column(Date(), nullable=False)
    end_date: Mapped[date] = mapped_column(Date(), nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default=BacktestStatus.PENDING.value)
    created_at: Mapped[datetime] = datetime_tz()
    server_data: Mapped[dict] = mapped_column(JSONB, nullable=True)

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="backtests")
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="backtest", cascade="all, delete-orphan"
    )
