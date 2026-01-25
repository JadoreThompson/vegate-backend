import sys
import time
import logging
from multiprocessing import Process, Queue

import click

from runners import BacktestListenerRunner, RunnerConfig, APIRunner
from runners.utils import run_runner

logger = logging.getLogger("commands.backend")


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

    # Create a queue for backtest jobs
    backtest_queue: Queue = Queue()

    configs = [
        RunnerConfig(
            cls=APIRunner,
            name="ServerRunner",
            args=(backtest_queue,),
            kwargs={
                "host": "0.0.0.0",
                "port": 8000,
                "reload": False,
                "workers": workers,
            },
        ),
        RunnerConfig(
            cls=BacktestListenerRunner,
            name="BacktestListenerRunner",
            args=(backtest_queue,),
        ),
    ]

    ps: list[Process] = [
        Process(
            target=run_runner,
            name=config.name,
            args=(config,),
        )
        for config in configs
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
