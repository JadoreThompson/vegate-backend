import asyncio
import logging
from uuid import UUID

import click

from core.redis import REDIS_CLIENT_SYNC
from core.redis.client import REDIS_CLIENT
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.monitor import DeploymentEventMonitorService
from module.event_bus import SyncEventPublisher
from module.event_bus.publisher.publisher import EventPublisher
from module.health.server import HealthCheckServer
from module.markets.feed import OHLCFeedClient
from module.deployment.runner import StrategyDeploymentRunner
from module.deployment.oms import OMSClient

logger = logging.getLogger("commands.deployment")


@click.group()
def deployment():
    """Manage live strategy deployments."""
    pass


@deployment.command(name="run")
@click.option(
    "--deployment-id",
    type=UUID,
    required=True,
    help="UUID of the strategy deployment to run",
)
@click.option("--ohlc-feed-host", type=str, required=True)
@click.option("--ohlc-feed-port", type=int, required=True)
@click.option("--oms-base-url", type=str, required=True)
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def run(deployment_id, ohlc_feed_host, ohlc_feed_port, oms_base_url, verbose):
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ohlc_feed_client = OHLCFeedClient(host=ohlc_feed_host, port=ohlc_feed_port)
    oms_client = OMSClient(base_url=oms_base_url)
    event_publisher = SyncEventPublisher()

    runner = StrategyDeploymentRunner(
        deployment_id=deployment_id,
        ohlc_feed_client=ohlc_feed_client,
        oms_client=oms_client,
        event_publisher=event_publisher,
        redis_client=REDIS_CLIENT_SYNC,
    )
    runner.run()


@deployment.group(name="monitor")
def monitor():
    """Monitor live strategy deployments."""
    return


@monitor.command(name="run")
def run():
    monitor_service = DeploymentEventMonitorService(
        deserialiser=DeploymentEventDeserialiser(),
        redis_client=REDIS_CLIENT,
        event_publisher=EventPublisher(),
    )
    monitor_service.setup()
    
    health_server = HealthCheckServer()

    async def _run():
        await asyncio.gather(monitor_service.run(), health_server.run_forever())
    
    asyncio.run(_run())
