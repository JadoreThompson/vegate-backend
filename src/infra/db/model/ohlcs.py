import uuid
from datetime import datetime

from sqlalchemy import Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from enums import BrokerType, Timeframe
from .base import Base, datetime_tz, uuid_pk


class OHLCs(Base):
    __tablename__ = "ohlc_levels"
    __table_args__ = (Index("idx_ohlc_levels_source_symbol", "source", "symbol"),)

    ohlc_id: Mapped[uuid.UUID] = uuid_pk()
    source: Mapped[BrokerType] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    open: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(20, 8), nullable=False)
    timeframe: Mapped[Timeframe] = mapped_column(String, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()
