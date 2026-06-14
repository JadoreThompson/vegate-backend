import asyncio
import logging
import sys

import click

from config import BACKTEST_EXECUTOR_NAME, MAX_CONCURRENT_BACKTESTS
from core.redis import REDIS_CLIENT, REDIS_CLIENT_SYNC
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.executor import BacktestExecutorFactory
from module.backtest.manager import (
    BacktestManager,
    BacktestEventHandler,
    BacktestMonitor,
    BacktestState,
)
from module.backtest.runner import BacktestRunner
from module.event_bus import OutboxEventPublisher, SyncOutboxEventPublisher
from module.health.server import HealthCheckServer
from module.notification.publisher import NotificationPublisher

logger = logging.getLogger("commands.backtest")


@click.group()
def backtest():
    """Manage backtests."""
    pass


@backtest.command(name="run")
@click.option("--backtest-id", required=True, help="UUID of the backtest to run")
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def backtest_run(backtest_id, verbose):
    """
    Run a backtest by its ID.

    The backtest configuration must already exist in the database.

    Examples:
      vegate backtest run --backtest-id 123e4567-e89b-12d3-a456-426614174000
      vegate backtest run --backtest-id 123e4567-e89b-12d3-a456-426614174000 --verbose
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo(f"Starting backtest: {backtest_id}")

    try:
        runner = BacktestRunner(
            backtest_id=backtest_id,
            event_publisher=SyncOutboxEventPublisher(),
            redis_client=REDIS_CLIENT_SYNC,
        )
        runner.run()
        click.echo("Backtest completed successfully")
    except KeyboardInterrupt:
        click.echo("\nBacktest stopped by user")
    except Exception as e:
        click.echo(f"Error running backtest: {e}", err=True)
        logger.exception("Backtest failed")
        sys.exit(1)


@backtest.group(name="monitor")
def monitor():
    """Backtest monitor command group."""
    return


@monitor.command(name="run")
@click.option("--health-port", type=int, default=5555, help="Health check server port")
def monitor_run(health_port):
    backtest_executor = BacktestExecutorFactory.create(BACKTEST_EXECUTOR_NAME)
    backtest_executor.max_concurrent_backtests = MAX_CONCURRENT_BACKTESTS

    state = BacktestState()
    event_publisher = OutboxEventPublisher()

    event_handler = BacktestEventHandler(
        state=state,
        deserialiser=BacktestEventDeserialiser(),
        event_publisher=event_publisher,
        backtest_executor=backtest_executor,
        notification_publisher=NotificationPublisher(),
    )
    monitor = BacktestMonitor(
        state=state,
        redis_client=REDIS_CLIENT,
        event_publisher=event_publisher,
    )
    manager = BacktestManager(event_handler=event_handler, monitor=monitor)

    health_server = HealthCheckServer(port=health_port)

    async def _run():
        manager.setup()
        await asyncio.gather(manager.run(), health_server.run_forever())

    asyncio.run(_run())
