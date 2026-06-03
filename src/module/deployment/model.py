from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    UUID,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.db import Base, datetime_tz, uuid_pk
from module.broker.enums import OrderSide, OrderStatus
from module.deployment.enums import StrategyDeploymentStatus
from module.markets.enums import Timeframe
from util import get_datetime

from .event import DeploymentEventType


class StrategyDeployments(Base):
    __tablename__ = "strategy_deployments"

    deployment_id: Mapped[uuid.UUID] = uuid_pk()

    version_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_versions.id", name="fk_strategy_versions_id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("broker_connections.connection_id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[StrategyDeploymentStatus] = mapped_column(
        String,
        nullable=False,
        default=StrategyDeploymentStatus.PENDING.value,
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
    stopped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    server_data: Mapped[dict] = mapped_column(JSONB, nullable=True)
    service_id: Mapped[str | None] = mapped_column(String, nullable=True)

    metrics: Mapped["StrategyDeploymentMetrics"] = relationship(
        back_populates="deployment",
        cascade="all, delete-orphan",
    )


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

    deployment: Mapped["StrategyDeployments"] = relationship(
        back_populates="metrics",
    )


class StrategyDeploymentOrders(Base):
    __tablename__ = "strategy_deployment_orders"

    id: Mapped[uuid.UUID] = uuid_pk()

    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    symbol: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    filled_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    notional: Mapped[float] = mapped_column(Float, nullable=True)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[OrderSide] = mapped_column(String, nullable=False)
    limit_price: Mapped[float] = mapped_column(Float, nullable=True)
    stop_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[OrderStatus] = mapped_column(String, nullable=False)

    broker_order_id: Mapped[str] = mapped_column(
        String,
        nullable=True,
        index=True,
    )
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)


class DeploymentEvent(Base):
    __tablename__ = "deployment_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[DeploymentEventType] = mapped_column(
        String,
        nullable=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=True)