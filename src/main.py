import logging
import time
from multiprocessing import Process
from typing import Type

from config import RAILWAY_API_KEY, RAILWAY_PROJECT_ID
from runners import BaseRunner, ServerRunner


def run_runner(runner_cls: Type[BaseRunner], *args, **kw):
    runner = runner_cls(*args, **kw)
    runner.run()


def main():
    logger = logging.getLogger("main")

    configs = ((ServerRunner, (), {"host": "0.0.0.0", "port": 8000, "reload": False}),)

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
    finally:
        for p in ps:
            if p.is_alive():
                logger.info(f"Shutting down process '{p.name}'...")
                p.kill()
                p.join()
        logger.info("All processes shut down.")


if __name__ == "__main__":
    main()
