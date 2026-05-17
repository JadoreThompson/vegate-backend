import uuid
from typing import TYPE_CHECKING

from sqlalchemy import UUID, Float, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infra.db.model.base import Base, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.strategy_deployments import StrategyDeployments


class StrategyDeploymentMetrics(Base):
    __tablename__ = "strategy_deployment_metrics"

    id: Mapped[uuid.UUID] = uuid_pk()
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    realised_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    unrealised_pnl: Mapped[float] = mapped_column(Float, nullable=False)
    total_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    profit_factor: Mapped[float] = mapped_column(Float, nullable=False)
    total_orders: Mapped[int] = mapped_column(Integer, nullable=False)

    # Relationships
    deployment: Mapped["StrategyDeployments"] = relationship(back_populates="metrics")
