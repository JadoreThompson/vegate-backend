import uvicorn
from multiprocessing import Queue
from multiprocessing.queues import Queue as QueueT

from api.backtest_queue import set_backtest_queue
from .base import BaseRunner


class APIRunner(BaseRunner):
    """Runs the FastAPI server using uvicorn."""

    def __init__(self, backtest_queue: QueueT | None = None, **uvicorn_kwargs):
        self._backtest_queue = backtest_queue
        self._kw = uvicorn_kwargs

    def run(self) -> None:
        # Set the backtest queue in the API module if provided
        if self._backtest_queue is not None:
            set_backtest_queue(self._backtest_queue)

        uvicorn.run("api.app:app", **self._kw)
