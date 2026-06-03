import asyncio
import click

from core.kafka import AsyncKafkaProducer
from module.event_bus.outbox_poller import OutboxPoller
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
@click.option("--health-port", type=int, default=5555, help="Health check server port")
def run(interval, batch_size, timeout, health_port):
    async def _run():
        kafka_producer = AsyncKafkaProducer()

        try:
            await kafka_producer.start()

            outbox_service = OutboxPoller(
                interval=interval,
                batch_size=batch_size,
                kafka_producer=kafka_producer,
                timeout=timeout,
            )

            health_server = HealthCheckServer(host="0.0.0.0", port=health_port)
            await asyncio.gather(outbox_service.run(), health_server.run_forever())
        finally:
            await kafka_producer.stop()

    try:
        asyncio.run(_run())
    except KeyError:
        pass
