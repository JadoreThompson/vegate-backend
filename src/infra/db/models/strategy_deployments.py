from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, String, Text, UUID as SaUUID, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from engine.enums import MarketType
from core.enums import DeploymentStatus
from .base import Base, datetime_tz, uuid_pk
from utils import get_datetime

if TYPE_CHECKING:
    from .strategies import Strategies
    from .broker_connections import BrokerConnections
    from .orders import Orders


class StrategyDeployments(Base):
    __tablename__ = "strategy_deployments"

    deployment_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_connection_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("broker_connections.connection_id", ondelete="CASCADE"),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    starting_balance: Mapped[float] = mapped_column(Float, nullable=True)
    status: Mapped[DeploymentStatus] = mapped_column(
        String, nullable=False, default=DeploymentStatus.PENDING.value
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
    stopped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    server_data: Mapped[dict] = mapped_column(JSONB, nullable=True)

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="strategy_deployments")
    broker_connection: Mapped["BrokerConnections"] = relationship(
        back_populates="strategy_deployments"
    )
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="deployment", cascade="all, delete-orphan"
    )
