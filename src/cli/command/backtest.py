import asyncio
import logging
import sys

import click

from config import BACKTEST_EXECUTOR_NAME, EMAIL_SERVICE_NAME
from core.redis import REDIS_CLIENT, REDIS_CLIENT_SYNC
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.executor import BacktestExecutorFactory
from module.backtest.monitor import BacktestMonitor
from module.backtest.runner import BacktestRunner
from module.email import EmailServiceFactory
from module.event_bus import OutboxEventPublisher, SyncOutboxEventPublisher
from module.health.server import HealthCheckServer
from module.notification.channel import (
    EmailNotificationChannel,
    NotificationChannelType,
)
from module.notification.publisher import NotificationPublisher
from module.notification.template import EmailNotificationTemplateEngine

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
def monitor_run():
    monitor_service = BacktestMonitor(
        deserialiser=BacktestEventDeserialiser(),
        redis_client=REDIS_CLIENT,
        event_publisher=OutboxEventPublisher(),
        backtest_executor=BacktestExecutorFactory.create(BACKTEST_EXECUTOR_NAME),
        notification_publisher=NotificationPublisher(),
    )
    monitor_service.setup()

    health_server = HealthCheckServer()

    async def _run():
        await asyncio.gather(monitor_service.run(), health_server.run_forever())

    asyncio.run(_run())
