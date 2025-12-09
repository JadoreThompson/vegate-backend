"""Backend server commands."""

import sys
import time
import logging
from multiprocessing import Process
from typing import Type

import click

from runners import BaseRunner, ServerRunner

logger = logging.getLogger(__name__)


def run_runner(runner_cls: Type[BaseRunner], *args, **kw):
    """Helper function to run a runner in a separate process."""
    runner = runner_cls(*args, **kw)
    runner.run()


@click.group()
def backend():
    """Manage the backend server."""
    pass


@backend.command(name="run")
@click.option(
    "--workers",
    default=None,
    type=int,
    help="Number of worker processes",
    show_default=True,
)
def backend_run(workers):
    """
    Run the backend server.
    """
    logger.info("Starting Vegate Backend")

    configs = (
        (
            ServerRunner,
            (),
            {"host": "0.0.0.0", "port": 8000, "reload": False, "workers": workers},
        ),
    )

    ps: list[Process] = [
        Process(
            target=run_runner,
            name=runner_cls.__name__,
            args=(runner_cls, *c_args),
            kwargs=c_kwargs,
        )
        for runner_cls, c_args, c_kwargs in configs
    ]

    for p in ps:
        p.start()
        logger.info(f"Started process '{p.name}' (PID: {p.pid}).")

    exit_code = 1
    try:
        while True:
            for _, p in enumerate(ps):
                if not p.is_alive():
                    raise RuntimeError(
                        f"Process '{p.name}' has died (exit code: {p.exitcode})."
                    )
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt — shutting down all processes.")
        click.echo("\nServer stopped by user")
        exit_code = 0
    except Exception as e:
        logger.exception("Error running server")
        click.echo(f"Error: {e}", err=True)
        exit_code = 1
    finally:
        for p in ps:
            if p.is_alive():
                logger.info(f"Shutting down process '{p.name}'...")
                p.kill()
                p.join()
        logger.info("All processes shut down.")
        sys.exit(exit_code)
