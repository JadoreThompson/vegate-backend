from datetime import datetime
from uuid import uuid4

from sqlalchemy import UUID, String, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
 
from utils.db import get_datetime


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = "users"

    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=get_datetime
    )
