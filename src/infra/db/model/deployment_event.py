import uuid
from datetime import datetime

from sqlalchemy import UUID, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from events.deployment import DeploymentEventType
from .base import Base


class DeploymentEvent(Base):

    __tablename__ = "deployment_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[DeploymentEventType] = mapped_column(String, nullable=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=True)
