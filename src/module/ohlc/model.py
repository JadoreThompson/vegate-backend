import uuid
from datetime import datetime

from sqlalchemy import UUID, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.db import Base, datetime_tz, uuid_pk
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe


class Instrument(Base):
    __tablename__ = "instruments"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "market_type",
            "broker_type",
            name="unq_symbol_market_type_broker_type",
        ),
    )

    id: Mapped[uuid.UUID] = uuid_pk()
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    # Native to the exchange / broker
    native_symbol: Mapped[str] = mapped_column(String, nullable=False)
    broker_type: Mapped[BrokerType] = mapped_column(String, nullable=False)
    market_type: Mapped[MarketType] = mapped_column(String, nullable=False)

    # Relationships
    ohlcs: Mapped[list["OHLC"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )


class OHLC(Base):
    __tablename__ = "ohlcs"

    ohlc_id: Mapped[uuid.UUID] = uuid_pk()
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("instruments.id", ondelete="CASCADE"),
        nullable=False,
    )
    volume: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    open: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()

    # Relationships
    instrument: Mapped["Instrument"] = relationship(back_populates="ohlcs")
