import logging
from multiprocessing import Queue
from uuid import UUID

from .backtest_runner import BacktestRunner
from .base import BaseRunner
from .deployment_runner import DeploymentRunner


class BacktestListenerRunner(BaseRunner):
    """Listens for backtest and deployment jobs on a queue and executes them."""

    def __init__(self, backtest_queue: Queue):
        """Initialize BacktestListenerRunner.

        Args:
            backtest_queue: Multiprocessing Queue to receive job messages.
                           Messages should be dictionaries with either:
                           - {"backtest_id": <uuid>} for backtest jobs
                           - {"deployment_id": <uuid>} for deployment jobs
        """
        self._queue = backtest_queue
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """Listen for backtest and deployment jobs on the queue and execute them."""
        self._logger.info("BacktestListenerRunner started, listening for jobs...")

        while True:
            try:
                # Block until a job is received from the queue
                job = self._queue.get()

                if job is None:
                    # None is a sentinel value to stop the listener
                    self._logger.info("Received stop signal, shutting down BacktestListenerRunner")
                    break

                # Handle backtest jobs
                if isinstance(job, dict) and "backtest_id" in job:
                    backtest_id = UUID(job["backtest_id"])
                    self._logger.info(f"Received backtest job for ID: {backtest_id}")

                    try:
                        runner = BacktestRunner(backtest_id)
                        runner.run()
                    except Exception as e:
                        self._logger.error(
                            f"Error processing backtest {backtest_id}",
                            exc_info=e,
                        )

                # Handle deployment jobs
                elif isinstance(job, dict) and "deployment_id" in job:
                    deployment_id = UUID(job["deployment_id"])
                    self._logger.info(f"Received deployment job for ID: {deployment_id}")

                    try:
                        runner = DeploymentRunner(deployment_id)
                        runner.run()
                    except Exception as e:
                        self._logger.error(
                            f"Error processing deployment {deployment_id}",
                            exc_info=e,
                        )

                else:
                    self._logger.warning(
                        f"Received invalid job format. Expected dict with 'backtest_id' or 'deployment_id', got: {job}"
                    )

            except Exception as e:
                self._logger.error(
                    f"Error in listener loop",
                    exc_info=e,
                )
