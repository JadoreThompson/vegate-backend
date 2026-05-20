from enum import Enum
import json
from typing import Literal
from uuid import UUID

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
    broker_order_id: UUID


class DeploymentModifyOrderSubmitted(_DeploymentEvent):
    type: Literal[DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED] = (
        DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED
    )
    order_id: UUID
    broker_order_id: UUID
    limit_price: float
    stop_price: float


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
    broker_order_id: UUID


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


class DeploymentEventDeserialiser:

    def __init__(self):
        self._registry = {
            DeploymentEventType.DEPLOYMENT_STATUS: DeploymentStatusChangedEvent,
            DeploymentEventType.DEPLOYMENT_ERROR: DeploymentErrorEvent,
            DeploymentEventType.DEPLOYMENT_STOP_REQUESTED: DeploymentStopRequestedEvent,
            DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED: DeploymentOrderSubmitted,
            DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED: DeploymentCancelOrderSubmitted,
            DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED: DeploymentModifyOrderSubmitted,
            DeploymentEventType.DEPLOYMENT_ORDER_REJECTED: DeploymentOrderRejected,
            DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED: DeploymentOrderAcknowledged,
        }

    def deserialise_json(self, payload: str | bytes):
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        data = json.loads(payload)
        return self.deserialise(data)

    def deserialise(self, data: dict):
        try:
            event_type = DeploymentEventType(data["type"])
        except KeyError:
            raise ValueError("Missing event type field")
        except ValueError:
            raise ValueError(f"Unknown event type '{data['type']}'")

        model = self._registry[event_type]

        return model.model_validate(data)
