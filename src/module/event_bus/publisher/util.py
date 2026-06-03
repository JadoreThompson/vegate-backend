from core.event import BaseEvent


def build_headers(event: BaseEvent):
    headers = [("event_type", event.type.value.encode())]

    if event.type.value.startswith("deployment."):
        headers.append(("deployment_id", str(event.deployment_id).encode()))

    return headers