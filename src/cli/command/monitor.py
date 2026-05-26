import asyncio
import click

from core.redis import REDIS_CLIENT
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.monitor import BacktestMonitor
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


@monitor.command(name="run-backtest")
def run_backtest():
    monitor_service = BacktestMonitor(
        deserialiser=BacktestEventDeserialiser(),
        redis_client=REDIS_CLIENT,
        event_publisher=EventPublisher(),
    )

    monitor_service.setup()
    asyncio.run(monitor_service.run())
