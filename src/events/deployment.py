from enum import Enum
from typing import Literal
from uuid import UUID

from enums import StrategyDeploymentStatus
from events.base import BaseEvent


class DeploymentEventType(str, Enum):
    """Strategy event type enumeration."""

    DEPLOYMENT_ERROR = "deployment.error"
    DEPLOYMENT_STATUS = "deployment.status"


class _DeploymentEvent(BaseEvent):
    type: DeploymentEventType
    deployment_id: UUID


class DeploymentStatusChangedEvent(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_STATUS] = (
        DeploymentEventType.DEPLOYMENT_STATUS
    )
    status: StrategyDeploymentStatus


class DeploymentErrorEvent(_DeploymentEvent):
    """Event for when a strategy encounters an error."""

    type: Literal[DeploymentEventType.DEPLOYMENT_ERROR] = (
        DeploymentEventType.DEPLOYMENT_ERROR
    )
    error_msg: str


class DeploymentStopRequestedEvent(_DeploymentEvent): ...
