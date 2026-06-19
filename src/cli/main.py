import sys

import click

from cli.command.backtest import backtest
from cli.command.db import db
from cli.command.http import http
from cli.command.markets import markets
from cli.command.oms import oms
from cli.command.outbox import outbox
from cli.command.deployment import deployment
from cli.command.notification import notification
from cli.command.yaml_cmd import yaml_cmd


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
cli.add_command(db)
cli.add_command(http)
cli.add_command(markets)
cli.add_command(oms)
cli.add_command(outbox)
cli.add_command(deployment)
cli.add_command(notification)
cli.add_command(yaml_cmd)
