import sys
import logging

import click

from runners import EventRunner

logger = logging.getLogger("commands.events")


@click.group()
def events():
    """Manage order event processing."""
    pass


@events.command(name="run")
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def events_run(verbose):
    """
    Run the order event handler to listen for and process order events.

    This command starts a long-running process that listens to the order
    event stream and processes events as they arrive.

    Examples:
      vegate events run
      vegate events run --verbose
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo("Starting order event handler")
    click.echo("Press Ctrl+C to stop the event handler")

    try:
        runner = EventRunner()
        runner.run()
        click.echo("Event handler stopped")
    except KeyboardInterrupt:
        click.echo("\nEvent handler stopped by user")
    except Exception as e:
        click.echo(f"Error running event handler: {e}", err=True)
        logger.exception("Event handler failed")
        sys.exit(1)
