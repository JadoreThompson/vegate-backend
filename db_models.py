from datetime import datetime
from uuid import uuid4

from sqlalchemy import UUID, DateTime, Float, String, ForeignKey, Integer
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, relationship

from utils import get_datetime


class Base(DeclarativeBase): ...


class Users(Base):
    __tablename__ = "users"

    user_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String, nullable=False)