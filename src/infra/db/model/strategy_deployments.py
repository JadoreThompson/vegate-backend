from datetime import datetime
from typing import TYPE_CHECKING
import uuid

from sqlalchemy import DateTime, ForeignKey, String, Text, UUID, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from enums import StrategyDeploymentStatus, MarketType, Timeframe, BrokerType
from utils import get_datetime
from .base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from .strategy import Strategy
    from .broker_connections import BrokerConnections
    from .orders import Orders
    from .account_snapshots import AccountSnapshots
    from infra.db.model.strategy_deployment_metrics import StrategyDeploymentMetrics


class StrategyDeployments(Base):
    __tablename__ = "strategy_deployments"

    deployment_id: Mapped[uuid.UUID] = uuid_pk()
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy.strategy_id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("broker_connections.connection_id", ondelete="CASCADE"),
        nullable=False,
    )
    # symbol: Mapped[str] = mapped_column(String, nullable=False)
    # broker: Mapped[BrokerType] = mapped_column(String, nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    # market_type: Mapped[MarketType] = mapped_column(String, nullable=False)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[StrategyDeploymentStatus] = mapped_column(
        String, nullable=False, default=StrategyDeploymentStatus.PENDING.value
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
    stopped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    server_data: Mapped[dict] = mapped_column(JSONB, nullable=True)
    service_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Relationships
    strategy: Mapped["Strategy"] = relationship(back_populates="strategy_deployments")
    broker_connection: Mapped["BrokerConnections"] = relationship(
        back_populates="strategy_deployments"
    )
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="deployment", cascade="all, delete-orphan"
    )
    account_snapshots: Mapped[list["AccountSnapshots"]] = relationship(
        back_populates="deployment", cascade="all, delete-orphan"
    )
    metrics: Mapped["StrategyDeploymentMetrics"] = relationship(
        back_populates="deployment", cascade="all, delete-orphan"
    )
