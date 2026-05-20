import asyncio
import logging

import click

from events.deployment import DeploymentEventDeserialiser
from service.event.consumer import EventConsumerService

logger = logging.getLogger("commands.consumer")


@click.group()
def consumer():
    """Consume and persist strategy deployment events."""
    pass


@consumer.command(name="run")
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def run(verbose):
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo("Starting consumer")
    click.echo("Press Ctrl+C to stop the event handler")

    try:
        deserialiser = DeploymentEventDeserialiser()
        service = EventConsumerService(deserialiser=deserialiser)
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass
