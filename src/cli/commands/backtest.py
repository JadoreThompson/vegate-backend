"""Backtest commands."""

import sys
import logging

import click

from runners import BacktestRunner

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
        runner = BacktestRunner(backtest_id=backtest_id)
        runner.run()
        click.echo("Backtest completed successfully")
    except KeyboardInterrupt:
        click.echo("\nBacktest stopped by user")
    except Exception as e:
        click.echo(f"Error running backtest: {e}", err=True)
        logger.exception("Backtest failed")
        sys.exit(1)
