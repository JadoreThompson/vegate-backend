import uvicorn
from multiprocessing import Queue
from multiprocessing.queues import Queue as QueueT

from .base import BaseRunner


class APIRunner(BaseRunner):
    """Runs the FastAPI server using uvicorn."""

    def __init__(self, uvicorn_kw):
        self._uvicorn_kw = uvicorn_kw

    def run(self) -> None:
        uvicorn.run("api.app:app", **self._uvicorn_kw)
