import uuid
from typing import TYPE_CHECKING

from sqlalchemy import UUID, Float, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infra.db.model.base import Base, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.backtest import Backtest


class BacktestMetrics(Base):
    __tablename__ = "backtest_metrics"

    id: Mapped[uuid.UUID] = uuid_pk()
    backtest_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtests.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    realised_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    unrealised_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    total_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    profit_factor: Mapped[float] = mapped_column(Float, nullable=False)
    total_orders: Mapped[int] = mapped_column(Integer, nullable=False)
    equity_curve: Mapped[list] = mapped_column(JSONB, nullable=False)
    balance_curve: Mapped[list] = mapped_column(JSONB, nullable=False)

    # Relationships
    backtest: Mapped["Backtest"] = relationship(back_populates="metrics")
