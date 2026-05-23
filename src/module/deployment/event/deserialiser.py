import json

from core.protocol import EventDeserialiser
from .event import (
    BaseDeploymentEvent,
    DeploymentCancelOrderSubmitted,
    DeploymentErrorEvent,
    DeploymentEventType,
    DeploymentModifyOrderSubmitted,
    DeploymentOrderAcknowledged,
    DeploymentOrderRejected,
    DeploymentOrderSubmitted,
    DeploymentStatusChangedEvent,
    DeploymentStopRequestedEvent,
)


class DeploymentEventDeserialiser(EventDeserialiser[BaseDeploymentEvent]):

    def __init__(self):
        self._registry: dict[DeploymentEventType, BaseDeploymentEvent] = {
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
