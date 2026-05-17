from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import String, Float, BigInteger, ForeignKey, JSON, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from enums import OrderStatus, OrderSide
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
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    filled_quantity: Mapped[float] = mapped_column(Float, nullable=False, default=text('0.0'))
    notional: Mapped[float] = mapped_column(Float, nullable=False)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[OrderSide] = mapped_column(String, nullable=False)
    limit_price: Mapped[float] = mapped_column(Float, nullable=False)
    stop_price: Mapped[float] = mapped_column(Float, nullable=False)
    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    candle_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[OrderStatus] = mapped_column(String, nullable=False, default=OrderStatus.PENDING)
    key: Mapped[str] = mapped_column(String, nullable=False)

    # Debug/audit payloads
    broker_order_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
