import uuid
from typing import TYPE_CHECKING

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from enums import BrokerType, MarketType
from infra.db.model.base import Base, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.ohlc import OHLC


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
