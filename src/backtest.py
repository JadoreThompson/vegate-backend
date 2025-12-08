import logging
import os
import sys

from runners.backtest_runner import BacktestRunner

logger = logging.getLogger(__name__)

BACKTEST_ID = os.getenv("BACKTEST_ID")

if not BACKTEST_ID:
    logger.error("BACKTEST_ID environment variable not found")
    sys.exit(1)

logger.info(f"Starting backtest with ID: {BACKTEST_ID}")

# Initialize and run the backtest runner
runner = BacktestRunner(BACKTEST_ID)
runner.run()

logger.info("Backtest completed")
