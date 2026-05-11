import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    Numeric,
    UUID,
)

from sqlalchemy.orm import Mapped, mapped_column, relationship

from infra.db.model.base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.backtest import Backtest


class BacktestEquityCurve(Base):
    """
    Stores the equity + balance time series for a backtest.

    One row per timestamp.

    Notes:
    - balance = realised account value
    - equity = balance + unrealised PnL
    - Use Numeric for financial precision
    """

    __tablename__ = "backtest_equity_curve"

    id: Mapped[uuid.UUID] = uuid_pk()
    backtest_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtests.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        index=True,
    )
    balance: Mapped[float] = mapped_column(
        Numeric(20, 8),
        nullable=False,
    )
    equity: Mapped[float] = mapped_column(
        Numeric(20, 8),
        nullable=False,
    )
    created_at: Mapped[datetime] = datetime_tz()

    # Relationships
    backtest: Mapped["Backtest"] = relationship(
        back_populates="equity_curve",
        passive_deletes=True,
    )
