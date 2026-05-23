import asyncio
import click

from module.event_bus import EventPublisher
from module.event_bus.outbox import OutboxService


@click.group
def outbox():
    return


@outbox.command
@click.option("--interval", required=True, type=int, help="Interal in seconds")
@click.option("--batch-size", required=True, type=int, help="Batch size")
@click.option(
    "--timeout",
    required=False,
    type=int,
    default=5,
    help="Timeout, how long to wait for each event to be emitted",
)
def run(interval, batch_size, timeout):
    event_publisher = EventPublisher()
    outbox_service = OutboxService(
        interval=interval,
        batch_size=batch_size,
        event_publisher=event_publisher,
        timeout=timeout,
    )

    try:
        asyncio.run(outbox_service.run())
    except KeyError:
        pass
