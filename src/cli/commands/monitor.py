import asyncio
import click

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from infra.kafka.client import KafkaConsumer
from infra.monitoring.deployment.service import DeploymentMonitoringService
from infra.redis.client import REDIS_CLIENT
from service.event.publisher import EventPublisher


@click.group(name="monitor")
def monitor():
    return


@monitor.command(name="run")
def run():
    monitor_service = DeploymentMonitoringService(
        redis_client=REDIS_CLIENT,
        event_publisher=EventPublisher(),
        heartbeat_prefix_key=STRATEGY_DEPLOYMENT_EVENTS_KEY,
    )

    asyncio.run(monitor_service.run())
