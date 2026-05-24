import logging
from uuid import UUID

import click

from core.redis import REDIS_CLIENT_SYNC
from module.event_bus import SyncEventPublisher
from module.markets.feed import OHLCFeedClient
from module.deployment.manager import StrategyDeploymentService
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

    sds = StrategyDeploymentService(
        deployment_id=deployment_id,
        ohlc_feed_client=ohlc_feed_client,
        oms_client=oms_client,
        event_publisher=event_publisher,
        redis_client=REDIS_CLIENT_SYNC,
    )
    sds.setup()
    sds.run()
