import uvicorn
from multiprocessing import Queue
from multiprocessing.queues import Queue as QueueT

from .base import BaseRunner


class APIRunner(BaseRunner):
    """Runs the FastAPI server using uvicorn."""

    def __init__(self, backtest_queue: QueueT | None = None, **uvicorn_kwargs):
        self._backtest_queue = backtest_queue
        self._kw = uvicorn_kwargs

    def run(self) -> None:
        uvicorn.run("api.app:app", **self._kw)
