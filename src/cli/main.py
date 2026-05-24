import sys

import click

from cli.command.backtest import backtest
from cli.command.consumer import consumer
from cli.command.db import db
from cli.command.feed import feed
from cli.command.http import http
from cli.command.monitor import monitor
from cli.command.ohlc_loader import ohlc_loader
from cli.command.oms import oms
from cli.command.outbox import outbox
from cli.command.deployment import deployment


@click.group(invoke_without_command=True)
@click.option("--version", is_flag=True, help="Show version information")
@click.pass_context
def cli(ctx, version):
    """
    Vegate Backend CLI
    """
    if version:
        click.echo("Vegate Backend CLI v0.1.0")
        click.echo("Python: " + sys.version.split()[0])
        ctx.exit()
    elif ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


cli.add_command(backtest)
cli.add_command(consumer)
cli.add_command(db)
cli.add_command(feed)
cli.add_command(http)
cli.add_command(monitor)
cli.add_command(ohlc_loader)
cli.add_command(oms)
cli.add_command(outbox)
cli.add_command(deployment)
