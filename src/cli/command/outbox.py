import asyncio
import click

from module.event_bus import EventPublisher
from module.event_bus.outbox import OutboxService
from module.health.server import HealthCheckServer


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
    
    health_server = HealthCheckServer(host="0.0.0.0")
    
    async def _run():
        await asyncio.gather(outbox_service.run(), health_server.run_forever())

    try:
        asyncio.run(_run())
    except KeyError:
        pass
