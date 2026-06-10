import asyncio
import logging
from uuid import UUID

import click

from config import DEPLOYMENT_EXECUTOR_NAME, MAX_CONCURRENT_DEPLOYMENTS
from core.redis import REDIS_CLIENT_SYNC, REDIS_CLIENT
from module.deployment.manager import DeploymentManager
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.executor import DeploymentExecutorFactory
from module.deployment.oms import OMSClient
from module.deployment.runner import StrategyDeploymentRunner
from module.event_bus import OutboxEventPublisher, SyncOutboxEventPublisher
from module.health.server import HealthCheckServer
from module.notification.publisher import NotificationPublisher
from vegate.markets.feed.client import OHLCFeedClient

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
def deployment_run(
    deployment_id, ohlc_feed_host, ohlc_feed_port, oms_base_url, verbose
):
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ohlc_feed_client = OHLCFeedClient(
        host=ohlc_feed_host,
        port=ohlc_feed_port,
        reconnect_attempts=5,
        reconnect_delay=10,
    )
    oms_client = OMSClient(base_url=oms_base_url)
    event_publisher = SyncOutboxEventPublisher()

    runner = StrategyDeploymentRunner(
        deployment_id=deployment_id,
        ohlc_feed_client=ohlc_feed_client,
        oms_client=oms_client,
        event_publisher=event_publisher,
        redis_client=REDIS_CLIENT_SYNC,
    )
    runner.run()


@deployment.group(name="listener")
def listener():
    """Monitor live strategy deployments."""
    return


@listener.command(name="run")
def listener_run():
    # Creating listener service
    deployment_executor = DeploymentExecutorFactory.create(DEPLOYMENT_EXECUTOR_NAME)
    deployment_executor.max_concurrent_deployments = MAX_CONCURRENT_DEPLOYMENTS

    manager_service = DeploymentManager(
        deserialiser=DeploymentEventDeserialiser(),
        redis_client=REDIS_CLIENT,
        event_publisher=OutboxEventPublisher(),
        notification_publisher=NotificationPublisher(),
        deployment_executor=deployment_executor,
    )

    health_server = HealthCheckServer()

    async def _run():
        await manager_service.setup()
        await asyncio.gather(manager_service.run(), health_server.run_forever())

    asyncio.run(_run())
