from enum import Enum
from typing import ClassVar, Literal
from uuid import UUID

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from enums import StrategyDeploymentStatus
from events.base import BaseEvent
from service.oms.broker_client.model import Order, OrderRequest


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


class _DeploymentEvent(BaseEvent):
    topic: ClassVar[str] = STRATEGY_DEPLOYMENT_EVENTS_KEY

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


class DeploymentStopRequestedEvent(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_STOP_REQUESTED] = (
        DeploymentEventType.DEPLOYMENT_STOP_REQUESTED
    )


class DeploymentOrderSubmitted(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED
    )
    order: OrderRequest


class DeploymentCancelOrderSubmitted(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED
    )
    order_id: UUID
    broker_order_id: str


class DeploymentModifyOrderSubmitted(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED
    )
    order_id: UUID
    broker_order_id: str
    limit_price: float | None = None
    stop_price: float | None = None


class DeploymentOrderRejected(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_REJECTED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_REJECTED
    )
    order_id: UUID


class DeploymentOrderAcknowledged(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED] = (
        DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED
    )
    order: Order
    broker_order_id: str


DeploymentEventT = (
    DeploymentStatusChangedEvent
    | DeploymentErrorEvent
    | DeploymentStopRequestedEvent
    | DeploymentOrderAcknowledged
    | DeploymentOrderRejected
    | DeploymentModifyOrderSubmitted
    | DeploymentCancelOrderSubmitted
    | DeploymentOrderSubmitted
)
