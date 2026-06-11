from datetime import datetime
from uuid import UUID

from sqlalchemy import ForeignKey, String, UUID as SaUUID
from sqlalchemy.orm import Mapped, mapped_column

from core.db import Base, datetime_tz, uuid_pk
from vegate.oms.enums import BrokerType
from util import get_datetime


class BrokerConnections(Base):
    __tablename__ = "broker_connections"

    id: Mapped[UUID] = uuid_pk()
    broker: Mapped[BrokerType] = mapped_column(String, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    api_key: Mapped[str | None] = mapped_column(String, nullable=True)
    secret_key: Mapped[str | None] = mapped_column(String, nullable=True)
    oauth_payload: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_account_id: Mapped[str] = mapped_column(String, nullable=False)
    broker_account_number: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = datetime_tz()
    updated_at: Mapped[datetime] = datetime_tz(onupdate=get_datetime)
