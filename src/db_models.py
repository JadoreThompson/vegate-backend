from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    UUID as SaUUID,
    BigInteger,
    Index,
    Integer,
    Numeric,
    String,
    DateTime,
    ForeignKey,
    Text,
    Date,
    UniqueConstraint,
    Float,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship

from core.enums import BacktestStatus, PricingTierType, StrategyDeploymentStatus
from engine.enums import BrokerType, MarketType, Timeframe
from utils.utils import get_datetime
from utils.utils import get_uuid


def uuid_pk(**kw):
    """Helper function for UUID primary key columns."""
    return mapped_column(SaUUID(as_uuid=True), primary_key=True, default=get_uuid, **kw)


def datetime_tz(**kw):
    """Helper function for timezone-aware datetime columns."""
    if "nullable" not in kw:
        kw["nullable"] = False

    return mapped_column(DateTime(timezone=True), default=get_datetime, **kw)


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
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
    authenticated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Relationships
    broker_connections: Mapped[list["BrokerConnections"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    strategies: Mapped[list["Strategies"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class BrokerConnections(Base):
    __tablename__ = "broker_connections"

    connection_id: Mapped[UUID] = uuid_pk()
    broker: Mapped[BrokerType] = mapped_column(String, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    api_key: Mapped[str | None] = mapped_column(String, nullable=True)
    oauth_payload: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_account_id: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    user: Mapped["Users"] = relationship(back_populates="broker_connections")
    strategy_deployments: Mapped[list["StrategyDeployments"]] = relationship(
        back_populates="broker_connection", cascade="all, delete-orphan"
    )


class Strategies(Base):
    __tablename__ = "strategies"
    __table_args__ = (UniqueConstraint("user_id", "name"),)

    strategy_id: Mapped[UUID] = uuid_pk()
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(nullable=False, onupdate=get_datetime)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    code: Mapped[str] = mapped_column(Text, nullable=False)
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Relationships
    backtests: Mapped[list["Backtests"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    strategy_deployments: Mapped[list["StrategyDeployments"]] = relationship(
        back_populates="strategy", cascade="all, delete-orphan"
    )
    user: Mapped["Users"] = relationship(back_populates="strategies")


class Backtests(Base):
    __tablename__ = "backtests"

    backtest_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("strategies.strategy_id"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    starting_balance: Mapped[float] = mapped_column(
        Float, nullable=False
    )
    metrics: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    start_date: Mapped[date] = mapped_column(Date(), nullable=False)
    end_date: Mapped[date] = mapped_column(Date(), nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    status: Mapped[BacktestStatus] = mapped_column(
        String, nullable=False, default=BacktestStatus.PENDING.value
    )
    created_at: Mapped[datetime] = datetime_tz()
    server_data: Mapped[dict] = mapped_column(JSONB, nullable=True)

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="backtests")
    orders: Mapped[list["Orders"]] = relationship(
        back_populates="backtest", cascade="all, delete-orphan"
    )


class StrategyDeployments(Base):
    __tablename__ = "strategy_deployments"

    deployment_id: Mapped[UUID] = uuid_pk()
    strategy_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("strategies.strategy_id"), nullable=False
    )
    broker_connection_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True),
        ForeignKey("broker_connections.connection_id"),
        nullable=False,
    )
    market_type: Mapped[MarketType] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    starting_balance: Mapped[float] = mapped_column(
        Float, nullable=True
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

    # Relationships
    strategy: Mapped["Strategies"] = relationship(back_populates="strategy_deployments")
    broker_connection: Mapped["BrokerConnections"] = relationship(
        back_populates="strategy_deployments"
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


class Ticks(Base):
    __tablename__ = "ticks"
    __table_args__ = (
        UniqueConstraint("source", "key"),
        Index("idx_ticks_source_timestamp", "source", "timestamp"),
    )

    tick_id: Mapped[UUID] = uuid_pk()
    source: Mapped[BrokerType] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    market_type: Mapped[MarketType] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()
    key: Mapped[str] = mapped_column(String, nullable=False)


class OHLCs(Base):
    __tablename__ = 'ohlc_levels'
    __table_args__ = (
        Index('idx_ohlc_levels_source_symbol', 'source', 'symbol'),
    )

    ohlc_id: Mapped[UUID] = uuid_pk()
    source: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    open: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()