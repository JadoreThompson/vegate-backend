from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import String, Float, BigInteger, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from enums import OrderStatus
from infra.db.model.base import Base, datetime_tz, uuid_pk
from utils import get_datetime


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
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    notional: Mapped[float | None] = mapped_column(Float, nullable=True)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    candle_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[OrderStatus] = mapped_column(String, nullable=False, default=OrderStatus.PENDING)
    key: Mapped[str] = mapped_column(String, nullable=False)

    # Debug/audit payloads
    broker_order_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    request_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)