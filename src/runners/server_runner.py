import uvicorn
from .base import BaseRunner


class ServerRunner(BaseRunner):
    """Runs the FastAPI server using uvicorn."""

    def __init__(self, **uvicorn_kwargs):
        self._kw = uvicorn_kwargs

    def run(self) -> None:
        uvicorn.run("api.app:app", **self._kw)
