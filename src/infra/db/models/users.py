from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, String, UUID as SaUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.enums import PricingTierType
from utils import get_datetime, get_uuid
from .base import Base, datetime_tz

if TYPE_CHECKING:
    from .broker_connections import BrokerConnections
    from .strategies import Strategies


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
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    strategies: Mapped[list["Strategies"]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
