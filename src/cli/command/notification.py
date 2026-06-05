import asyncio

import click

from config import EMAIL_SERVICE_NAME
from module.email import EmailServiceFactory
from module.health.server import HealthCheckServer
from module.notification.channel import (
    EmailNotificationChannel,
    NotificationChannelType,
)
from module.notification.poller import NotificationPoller
from module.notification.template import EmailNotificationTemplateEngine


@click.group()
def notification():
    """Manage notifications."""
    pass


@notification.group()
def poller():
    """Notification poller commands."""
    return


@poller.command()
@click.option("--interval", required=True, type=int, help="Polling interval in seconds")
@click.option("--batch-size", required=True, type=int, help="Batch size per poll cycle")
@click.option(
    "--timeout",
    required=False,
    type=int,
    default=30,
    help="Timeout in seconds for each notification to be sent",
)
@click.option("--health-port", type=int, default=5555, help="Health check server port")
def run(interval, batch_size, timeout, health_port):
    async def _run():
        email_service = EmailServiceFactory.create(
            EMAIL_SERVICE_NAME, "Vegate", "no-reply@vegate.jadore.dev"
        )
        email_channel = EmailNotificationChannel(
            email_service=email_service,
            template_engine=EmailNotificationTemplateEngine(),
        )
        notification_poller = NotificationPoller(
            notification_channels={NotificationChannelType.EMAIL: email_channel},
            interval=interval,
            batch_size=batch_size,
            timeout=timeout,
        )

        health_server = HealthCheckServer(host="0.0.0.0", port=health_port)
        await asyncio.gather(notification_poller.run(), health_server.run_forever())

    try:
        asyncio.run(_run())
    except KeyError:
        pass
