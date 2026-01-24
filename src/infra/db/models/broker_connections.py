from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import ForeignKey, String, UUID as SaUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from engine.enums import BrokerType
from .base import Base, uuid_pk

if TYPE_CHECKING:
    from .users import Users
    from .strategy_deployments import StrategyDeployments


class BrokerConnections(Base):
    __tablename__ = "broker_connections"

    connection_id: Mapped[UUID] = uuid_pk()
    broker: Mapped[BrokerType] = mapped_column(String, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        SaUUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False
    )
    api_key: Mapped[str | None] = mapped_column(String, nullable=True)
    secret_key: Mapped[str | None] = mapped_column(String, nullable=True)
    oauth_payload: Mapped[str | None] = mapped_column(String, nullable=True)
    broker_account_id: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    user: Mapped["Users"] = relationship(back_populates="broker_connections")
    strategy_deployments: Mapped[list["StrategyDeployments"]] = relationship(
        back_populates="broker_connection", cascade="all, delete-orphan"
    )
