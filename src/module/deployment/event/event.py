from enum import Enum
from typing import Annotated, ClassVar, Literal
from uuid import UUID

from pydantic import Field, RootModel

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from core.event import BaseEvent
from module.deployment.enums import StrategyDeploymentStatus
from vegate.oms.schema import Order, OrderRequest


class DeploymentEventType(str, Enum):
    """Strategy event type enumeration."""

    DEPLOYMENT_ERROR = "deployment.error"
    DEPLOYMENT_STATUS = "deployment.status"
    DEPLOYMENT_ORDER_ACKNOWLEDGED = "deployment.order_acknowledged"
    DEPLOYMENT_MODIFY_ORDER_SUBMITTED = "deployment.modify_order_submitted"
    DEPLOYMENT_ORDER_REJECTED = "deployment.ordere_rejected"
    DEPLOYMENT_CANCEL_ORDER_SUBMITTED = "deployment.cancel_order_submitted"
    DEPLOYMENT_STOP_REQUESTED = "deployment.stop_requested"
    DEPLOYMENT_ORDER_SUBMITTED = "deployment.order_submitted"
    DEPLOYMENT_REQUESTED = "deployment.requested"
    DEPLOYMENT_CANCELLED = "deployment.cancelled"


class BaseDeploymentEvent(BaseEvent):
    topic: ClassVar[str] = STRATEGY_DEPLOYMENT_EVENTS_KEY

    type: DeploymentEventType
    deployment_id: UUID


class DeploymentStatusChangedEvent(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_STATUS] = (
        DeploymentEventType.DEPLOYMENT_STATUS
    )
    status: StrategyDeploymentStatus


class DeploymentErrorEvent(BaseDeploymentEvent):
    """Event for when a strategy encounters an error."""

    type: Literal[DeploymentEventType.DEPLOYMENT_ERROR] = (
        DeploymentEventType.DEPLOYMENT_ERROR
    )
    error_msg: str


class DeploymentOrderSubmitted(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED
    )
    order: OrderRequest


class DeploymentCancelOrderSubmitted(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED
    )
    order_id: UUID
    broker_order_id: str


class DeploymentModifyOrderSubmitted(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED
    )
    order_id: UUID
    broker_order_id: str
    limit_price: float | None = None
    stop_price: float | None = None


class DeploymentOrderRejected(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_REJECTED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_REJECTED
    )
    order_id: UUID


class DeploymentOrderAcknowledged(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED
    )
    order: Order
    broker_order_id: str


class DeploymentRequestedEvent(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_REQUESTED] = (
        DeploymentEventType.DEPLOYMENT_REQUESTED
    )


class DeploymentStopRequestedEvent(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_STOP_REQUESTED] = (
        DeploymentEventType.DEPLOYMENT_STOP_REQUESTED
    )


class DeploymentCancelledEvent(BaseDeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_CANCELLED] = (
        DeploymentEventType.DEPLOYMENT_CANCELLED
    )
    reason: Literal["capacity_constraint"]


DeploymentEventUnion = (
    DeploymentStatusChangedEvent
    | DeploymentErrorEvent
    | DeploymentStopRequestedEvent
    | DeploymentOrderAcknowledged
    | DeploymentOrderRejected
    | DeploymentModifyOrderSubmitted
    | DeploymentCancelOrderSubmitted
    | DeploymentOrderSubmitted
    | DeploymentRequestedEvent
    | DeploymentCancelledEvent
)


class DeploymentEventT(RootModel[DeploymentEventUnion]):
    root: Annotated[DeploymentEventUnion, Field(discriminator="type")]
