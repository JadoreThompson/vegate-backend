import uvicorn
from .base_runner import BaseRunner


class ServerRunner(BaseRunner):
    """Runs the FastAPI server using uvicorn."""

    def __init__(self, host: str, port: int, reload: bool):
        super().__init__("Server")
        self.host = host
        self.port = port
        self.reload = reload

    def run(self) -> None:
        uvicorn.run(
            "server.app:app", host=self.host, port=self.port, reload=self.reload
        )
