import asyncio
import logging

from services.order_event_handler import OrderEventHandler
from .base import BaseRunner


class OrderEventRunner(BaseRunner):
    """Runs the order event handler to listen for and process order events."""

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._handler = OrderEventHandler()

    def run(self):
        """The main entry point for the order event listener process."""
        asyncio.run(self._run())

    async def _run(self) -> None:
        """Run the order event handler."""
        self._logger.info("Starting OrderEventRunner")
        try:
            await self._handler.listen()
        except asyncio.CancelledError:
            self._logger.info("OrderEventRunner cancelled")
            raise
        except Exception as e:
            self._logger.exception(f"Error in OrderEventRunner: {e}")
            raise
