import logging
from multiprocessing import Queue
from uuid import UUID

from .backtest_runner import BacktestRunner
from .base import BaseRunner


class BacktestListenerRunner(BaseRunner):
    """Listens for backtest jobs on a queue and executes them."""

    def __init__(self, backtest_queue: Queue):
        """Initialize BacktestListenerRunner.

        Args:
            backtest_queue: Multiprocessing Queue to receive backtest_id messages
        """
        self._queue = backtest_queue
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """Listen for backtest jobs on the queue and execute them."""
        self._logger.info("BacktestListenerRunner started, listening for backtest jobs...")

        while True:
            try:
                # Block until a backtest_id is received from the queue
                backtest_id = self._queue.get()

                if backtest_id is None:
                    # None is a sentinel value to stop the listener
                    self._logger.info("Received stop signal, shutting down BacktestListenerRunner")
                    break

                self._logger.info(f"Received backtest job for ID: {backtest_id}")

                # Instantiate and run the backtest
                runner = BacktestRunner(backtest_id)
                runner.run()

            except Exception as e:
                self._logger.error(
                    f"Error processing backtest from queue",
                    exc_info=e,
                )
