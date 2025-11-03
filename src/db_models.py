from datetime import datetime
from uuid import uuid4, UUID

from sqlalchemy import UUID as SaUUID, Integer, String, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped

from core.enums import PricingTierType
from utils.db import get_datetime


def get_uuid():
    return uuid4()


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
