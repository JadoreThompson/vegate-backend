from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, String, UUID as SaUUID, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infra.db.models.base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from infra.db.models.backtests import Backtests
    from infra.db.models.strategy_deployments import StrategyDeployments


class Orders(Base):
    __tablename__ = "orders"

    order_id: Mapped[UUID] = uuid_pk()
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[float] = mapped_column(
        Float, nullable=False
    )
    filled_quantity: Mapped[float] = mapped_column(
        Float, nullable=False, default=0
    )
    limit_price: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    stop_price: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    avg_fill_price: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    time_in_force: Mapped[str] = mapped_column(String, nullable=False, default="day")
    submitted_at: Mapped[datetime] = datetime_tz()
    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    client_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Foreign keys (nullable for backtest vs live)
    backtest_id: Mapped[UUID | None] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("backtests.backtest_id"), nullable=True
    )
    deployment_id: Mapped[UUID | None] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id"),
        nullable=True,
    )

    # Relationships
    backtest: Mapped["Backtests | None"] = relationship(back_populates="orders")
    deployment: Mapped["StrategyDeployments | None"] = relationship(
        back_populates="orders"
    )
