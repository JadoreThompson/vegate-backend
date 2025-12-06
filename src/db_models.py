from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    UUID as SaUUID,
    BigInteger,
    String,
    DateTime,
    ForeignKey,
    Text,
    Numeric,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship

from core.enums import PricingTierType
from engine.enums import BrokerPlatform, MarketType
from utils.utils import get_datetime
from utils.utils import get_uuid


# Helper functions for consistent column definitions
def uuid_pk(**kw):
    """Helper function for UUID primary key columns."""
    return mapped_column(SaUUID(as_uuid=True), primary_key=True, default=get_uuid, **kw)


def datetime_tz(nullable=False, **kw):
    """Helper function for timezone-aware datetime columns."""
    return mapped_column(
        DateTime(timezone=True), nullable=nullable, default=get_datetime, **kw
    )


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = "users"

    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), primary_key=True, default=get_uuid
    )
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String, nullable=False)
    jwt: Mapped[str] = mapped_column(String, nullable=True)
    pricing_tier: Mapped[str] = mapped_column(
        String, nullable=False, default=PricingTierType.FREE.value
    )
    stripe_customer_id: Mapped[str | None] = mapped_column(
        String, unique=True, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_datetime
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=get_datetime,
        onupdate=get_datetime,
    )
    authenticated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    broker_connections: Mapped[list["BrokerConnections"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class BrokerConnections(Base):
    __tablename__ = "broker_connections"

    connection_id: Mapped[UUID] = uuid_pk()
    broker: Mapped[BrokerPlatform] = mapped_column(String, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    api_key: Mapped[str | None] = mapped_column(String, nullable=True)
    oauth_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Relationships
    user: Mapped["Users"] = relationship(back_populates="broker_connections")
    live_deployments: Mapped[list["LiveDeployments"]] = relationship(
        back_populates="broker_connection", cascade="all, delete-orphan"
    )


class Strategies(Base):
    __tablename__ = "strategies"

    strategy_id: Mapped[UUID] = uuid_pk()
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=get_datetime,
        onupdate=get_datetime,
    )
    code: Mapped[str] = mapped_column(Text, nullable=False)

    # Relationships
    backtests: Mapped[list["Backtests"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    live_deployments: Mapped[list["LiveDeployments"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )


class Backtests(Base):
    __tablename__ = "backtests"

    backtest_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("strategies.strategy_id"), nullable=False
    )
    ticker: Mapped[str] = mapped_column(String, nullable=False)
    starting_balance: Mapped[Decimal] = mapped_column(
        Numeric(precision=15, scale=2), nullable=False
    )
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    status: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="backtests")
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="backtest", cascade="all, delete-orphan"
    )


class LiveDeployments(Base):
    __tablename__ = "live_deployments"

    deployment_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("strategies.strategy_id"), nullable=False
    )
    broker_connection_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("broker_connections.connection_id"),
        nullable=False,
    )
    created_at: Mapped[datetime] = datetime_tz()
    status: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="live_deployments")
    broker_connection: Mapped["BrokerConnections"] = relationship(
        back_populates="live_deployments"
    )
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="deployment", cascade="all, delete-orphan"
    )


class Orders(Base):
    __tablename__ = "orders"

    order_id: Mapped[UUID] = uuid_pk()
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    order_type: Mapped[str] = mapped_column(String, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(
        Numeric(precision=15, scale=8), nullable=False
    )
    filled_quantity: Mapped[Decimal] = mapped_column(
        Numeric(precision=15, scale=8), nullable=False, default=0
    )
    limit_price: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=15, scale=2), nullable=True
    )
    stop_price: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=15, scale=2), nullable=True
    )
    average_fill_price: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=15, scale=2), nullable=True
    )
    status: Mapped[str] = mapped_column(String, nullable=False)
    time_in_force: Mapped[str] = mapped_column(String, nullable=False, default="day")
    submitted_at: Mapped[datetime] = datetime_tz()
    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    client_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_order_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Foreign keys (nullable for backtest vs live)
    backtest_id: Mapped[UUID | None] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("backtests.backtest_id"), nullable=True
    )
    deployment_id: Mapped[UUID | None] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("live_deployments.deployment_id"),
        nullable=True,
    )

    # Relationships
    backtest: Mapped["Backtests | None"] = relationship(back_populates="orders")
    deployment: Mapped["LiveDeployments | None"] = relationship(back_populates="orders")


class Ticks(Base):
    __tablename__ = "ticks"
    __table_args__ = (UniqueConstraint("source", "key"),)

    tick_id: Mapped[UUID] = uuid_pk()
    source: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    market_type: Mapped[MarketType] = mapped_column(String, nullable=False)
    price: Mapped[Decimal] = mapped_column(
        Numeric(precision=15, scale=2), nullable=False
    )
    size: Mapped[int] = mapped_column(BigInteger, nullable=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()
    key: Mapped[str] = mapped_column(String, nullable=False)
