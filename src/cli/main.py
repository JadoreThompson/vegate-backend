import sys
import click

from cli.commands import (
    backend,
    backtest,
    deployment,
    db,
    events,
    feed,
    ohlc_loader,
    http,
)


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


cli.add_command(backend)
cli.add_command(backtest)
cli.add_command(deployment)
cli.add_command(db)
cli.add_command(events)
cli.add_command(http)
cli.add_command(ohlc_loader)
cli.add_command(feed)


if __name__ == "__main__":
    cli()
