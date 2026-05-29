import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, UUID
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, datetime_tz
from util import get_datetime, get_uuid


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=get_uuid
    )
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String, nullable=False)
    jwt: Mapped[str] = mapped_column(String, nullable=True)
    # TODO: Rename to email_verified_at
    authenticated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
