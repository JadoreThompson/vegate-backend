from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Float, ForeignKey, Integer, String, UUID, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.db import Base, uuid_pk, datetime_tz
from module.broker.enums import OrderStatus
from util import get_datetime
from .enums import BacktestStatus
from .event import BacktestEventType


class Backtest(Base):
    __tablename__ = "backtests"

    id: Mapped[uuid.UUID] = uuid_pk()
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy.strategy_id", ondelete="CASCADE"),
        nullable=False,
    )
    # Broker is needed to know which data source to use for backtesting
    starting_balance: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    end_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=BacktestStatus.PENDING.value
    )
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)

    # Relationships
    orders: Mapped[list["BacktestOrder"]] = relationship(
        back_populates="backtest", cascade="all, delete-orphan", passive_deletes=True
    )
    metrics: Mapped["BacktestMetrics"] = relationship(
        back_populates="backtest", cascade="all, delete-orphan", passive_deletes=True
    )


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


class BacktestOrder(Base):
    __tablename__ = "backtest_orders"

    id: Mapped[uuid.UUID] = uuid_pk()
    backtest_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtests.id", ondelete="CASCADE"),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[float | None] = mapped_column(Float, nullable=True)
    notional: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[OrderStatus] = mapped_column(String, nullable=False)
    submitted_at: Mapped[datetime] = datetime_tz()
    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Relationships
    backtest: Mapped["Backtest"] = relationship(back_populates="orders")


class BacktestEvent(Base):
    __tablename__ = "backtest_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
    )
    backtest_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("backtests.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[BacktestEventType] = mapped_column(
        String,
        nullable=True,
    )
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=True)
