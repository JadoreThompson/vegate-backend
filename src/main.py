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

    # import asyncio
    # from pipelines import AlpacaPipeline

    # async def func():

    #     pl = AlpacaPipeline()
    #     async with pl:
    #         await pl.run_crypto_pipeline("BTC/USD")

    # asyncio.run(func())

    # import asyncio

    # async def main():
    #     from uuid import uuid4
    #     from services import DeploymentService

    #     # Configuration
    #     service = DeploymentService(
    #         api_key=RAILWAY_API_KEY,
    #         project_id=RAILWAY_PROJECT_ID,
    #         docker_image="wifimemes/vegate-deploy:latest",  # Your Docker image
    #     )

    #     # Deploy with a specific deployment ID
    #     deployment_id = uuid4()

    #     try:
    #         result = await service.deploy(
    #             f"bt_{deployment_id}",
    #             {"BACKTEST_ID": str(deployment_id), "DEPLOYMENT_TYPE": "backtest"},
    #         )
    #         print("✓ Deployment successful!")
    #         print(f"  Service ID: {result['service_id']}")
    #         print(f"  Service Name: {result['service_name']}")
    #     except Exception as e:
    #         print(f"✗ Deployment failed: {e}")

    # asyncio.run(main())
