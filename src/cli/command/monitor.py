import asyncio
import click

from core.redis import REDIS_CLIENT
from module.event_bus import EventPublisher
from module.deployment.monitor import DeploymentMonitoringService


@click.group(name="monitor")
def monitor():
    return


@monitor.command(name="run")
def run():
    monitor_service = DeploymentMonitoringService(
        redis_client=REDIS_CLIENT, event_publisher=EventPublisher()
    )

    monitor_service.setup()
    asyncio.run(monitor_service.run())
