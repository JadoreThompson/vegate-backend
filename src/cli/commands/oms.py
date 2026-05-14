import sys
import logging
import time

import click

from service.oms.server.server import OMSServer
from service.oms.service import OMSService

logger = logging.getLogger("commands.oms")


@click.group()
def oms():
    """Manage the Order Management System (OMS) server."""
    pass


@oms.command(name="run")
@click.option("--host", default="0.0.0.0", help="Bind socket to this host")
@click.option("--port", default=8001, type=int, help="Bind socket to this port")
@click.option("--reload", is_flag=True, default=False, help="Enable auto-reload")
@click.option("--workers", default=1, type=int, help="Number of worker processes")
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def oms_run(host, port, reload, workers, verbose):
    """
    Launch the OMS Server.

    The OMS server provides a REST API for broker session management,
    order placement, modification, cancellation, and account queries.

    Examples:
      vegate oms run
      vegate oms run --host 127.0.0.1 --port 8001
      vegate oms run --reload --verbose
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo(f"Starting OMS server on {host}:{port}")

    try:
        oms_service = OMSService()
        uvicorn_kw = {
            "host": host,
            "port": port,
            "reload": reload,
            "workers": workers,
        }
        server = OMSServer(oms_service=oms_service, uvicorn_kw=uvicorn_kw)
        server.run()
    except KeyboardInterrupt:
        click.echo("\nOMS server stopped by user")
    except Exception as e:
        click.echo(f"Error running OMS server: {e}", err=True)
        logger.exception("OMS server failed")
        sys.exit(1)