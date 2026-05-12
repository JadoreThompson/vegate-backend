import logging
from multiprocessing import Process
import sys
import time
import click

from runners.api_runner import APIRunner
from runners.runner_config import RunnerConfig
from runners.utils import run_runner

logger = logging.getLogger(__name__)


@click.group("http")
def http():
    return


@http.command(name="run")
def run():
    runner_config = RunnerConfig(
        cls=APIRunner,
        args=(
            {
                "host": "0.0.0.0",
                "port": 8000,
                "reload": False,
            },
        ),
    )

    p = Process(target=run_runner, args=(runner_config,), name=runner_config.name)
    p.start()
    exit_code = 1

    try:
        while True:
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
        if p.is_alive():
            logger.info(f"Shutting down process '{p.name}'...")
            p.kill()
            p.join()
        logger.info("All processes shut down.")
        sys.exit(exit_code)
