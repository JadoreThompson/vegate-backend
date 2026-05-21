from events.base import BaseEvent
from events.deployment import DeploymentEventType


def build_headers(event: BaseEvent):
    headers = [("event_type", event.type.value.encode())]

    if isinstance(event.type, DeploymentEventType):
        headers.append(("deployment_id", str(event.deployment_id).encode()))

    return headers