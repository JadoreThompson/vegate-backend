import asyncio
import click

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from infra.kafka.client import KafkaConsumer
from infra.redis.client import REDIS_CLIENT
from service.event.publisher import EventPublisher
from service.monitoring.deployment.service import DeploymentMonitoringService


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
