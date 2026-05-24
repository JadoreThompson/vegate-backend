import asyncio
import click

from core.redis import REDIS_CLIENT
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.monitor import DeploymentEventMonitorService
from module.event_bus import EventPublisher


@click.group(name="monitor")
def monitor():
    return


@monitor.command(name="run")
def run():
    monitor_service = DeploymentEventMonitorService(
        deserialiser=DeploymentEventDeserialiser(),
        redis_client=REDIS_CLIENT,
        event_publisher=EventPublisher(),
    )

    monitor_service.setup()
    asyncio.run(monitor_service.run())
