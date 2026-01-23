from datetime import datetime
from uuid import UUID

from sqlalchemy import Index, Integer, Numeric, String, UUID as SaUUID
from sqlalchemy.orm import Mapped, mapped_column

from engine.enums import Timeframe
from infra.db.models.base import Base, datetime_tz, uuid_pk


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
