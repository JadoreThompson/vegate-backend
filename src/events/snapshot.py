from enum import Enum
from typing import Literal
from uuid import UUID

from enums import SnapshotType
from events.base import BaseEvent


class SnapshotEventType(str, Enum):
    """Snapshot event type enumeration."""

    SNAPSHOT_CREATED = "snapshot_created"


class SnapshotCreated(BaseEvent):
    """Event for when an account snapshot is created."""

    type: Literal[SnapshotEventType.SNAPSHOT_CREATED] = (
        SnapshotEventType.SNAPSHOT_CREATED
    )
    deployment_id: UUID
    snapshot_type: SnapshotType
    value: float
