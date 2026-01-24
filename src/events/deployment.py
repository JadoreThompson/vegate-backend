from enum import Enum
from typing import Literal
from uuid import UUID

from events.base import BaseEvent


class DeploymentEventType(str, Enum):
    """Strategy event type enumeration."""

    ERROR = "strategy_error"
    STOP = "stop"


class DeploymentErrorEvent(BaseEvent):
    """Event for when a strategy encounters an error."""

    type: Literal[DeploymentEventType.ERROR] = DeploymentEventType.ERROR
    deployment_id: UUID
    error_msg: str


class DeploymentStopEvent(BaseEvent):
    type: Literal[DeploymentEventType.STOP] = DeploymentEventType.STOP
    deployment_id: UUID
