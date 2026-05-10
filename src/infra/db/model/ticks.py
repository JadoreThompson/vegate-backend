from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, Index, String, UniqueConstraint, UUID, Float
from sqlalchemy.orm import Mapped, mapped_column

from enums import BrokerType
from .base import Base, datetime_tz, uuid_pk


class Ticks(Base):
    __tablename__ = "ticks"
    __table_args__ = (
        UniqueConstraint("source", "key"),
        Index("idx_ticks_source_timestamp", "source", "timestamp"),
    )

    tick_id: Mapped[UUID] = uuid_pk()
    source: Mapped[BrokerType] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = datetime_tz()
    key: Mapped[str] = mapped_column(String, nullable=False)
