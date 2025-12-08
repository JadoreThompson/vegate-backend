import logging
import os
import sys

from core.enums import DeploymentType
from runners import DeploymentRunner, BacktestRunner


logger = logging.getLogger(__name__)


def run_backtest():
    if not BACKTEST_ID:
        logger.error("BACKTEST_ID environment variable not found")
        sys.exit(1)

    logger.info(f"Starting backtest with ID: {BACKTEST_ID}")

    runner = BacktestRunner(BACKTEST_ID)
    runner.run()

    logger.info("Backtest completed")


def run_live():
    if not DEPLOYMENT_ID:
        logger.error("DEPLOYMENT_ID environment variable not found")
        sys.exit(1)

    logger.info(f"Starting deployment with ID: {DEPLOYMENT_ID}")

    runner = DeploymentRunner(DEPLOYMENT_ID)
    runner.run()

    logger.info("Deployment stopped")


def main():
    if DEPLOYMENT_TYPE == DeploymentType.BACKTEST:
        run_backtest()
    elif DEPLOYMENT_TYPE == DeploymentType.LIVE:
        run_live()
    else:
        logger.info(f"Unknown deployment type '{DEPLOYMENT_TYPE}'")


if __name__ == "__main__":
    # DEPLOYMENT_TYPE: DeploymentType = os.getenv("DEPLOYMENT_TYPE")
    DEPLOYMENT_TYPE: DeploymentType = DeploymentType.BACKTEST
    DEPLOYMENT_ID = os.getenv("DEPLOYMENT_ID")
    # BACKTEST_ID = os.getenv("BACKTEST_ID")
    import uuid

    BACKTEST_ID = uuid.uuid4()

    main()
