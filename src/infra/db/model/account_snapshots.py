import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String, Float, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from enums import SnapshotType
from .base import Base, datetime_tz, uuid_pk

if TYPE_CHECKING:
    from .strategy_deployments import StrategyDeployments


class AccountSnapshots(Base):
    __tablename__ = "account_snapshots"

    snapshot_id: Mapped[uuid.UUID] = uuid_pk()
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("strategy_deployments.deployment_id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = datetime_tz()
    snapshot_type: Mapped[SnapshotType] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    # Relationship
    deployment: Mapped["StrategyDeployments"] = relationship(
        back_populates="account_snapshots"
    )
