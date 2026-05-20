import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import UUID, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from enums import Timeframe

from .base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from infra.db.model.instrument import Instrument


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
