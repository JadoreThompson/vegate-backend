import logging
import signal
import sys

from engine.brokers import BaseBroker
from .base import BaseStrategy
from .context import StrategyContext


class StrategyManager:
    def __init__(self, strategy: BaseStrategy, broker: BaseBroker):
        self._broker = broker
        self._strategy = strategy

        self._is_running = False
        self._shutdown_called = False
        self._original_sigint_handler = None
        self._logger = logging.getLogger(type(self).__name__)

    @property
    def supports_async(self):
        return self._broker.supports_disconnect_async

    def __enter__(self):
        if self._broker.supports_disconnect_async:
            raise ValueError("Broker supports async. Use async context manager instead")
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    async def __aenter__(self):
        if not self._broker.supports_disconnect_async:
            raise ValueError(
                "Broker doesn't support async. Use sync context manager instead"
            )
        self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup_async()

    def on_candle(self, context: StrategyContext) -> None:
        try:
            self._strategy.on_candle(context)
        except Exception as e:
            self._logger.error(f"Error during strategy on candle : {e}", exc_info=True)

    def setup(self) -> None:
        """
        Set up the execution environment.

        This method:
        1. Registers signal handlers for graceful shutdown
        2. Connects to the broker
        3. Calls strategy.startup()

        Raises:
            Exception: If setup fails
        """
        try:
            self._original_sigint_handler = signal.signal(
                signal.SIGINT, self._signal_handler
            )

            self._logger.debug("Connecting to broker...")
            self._broker.connect()

            self._logger.debug("Calling strategy.startup()...")
            try:
                self._strategy.startup()
                self._logger.info("Strategy startup completed successfully")
            except Exception as e:
                self._logger.error(f"Strategy startup failed: {e}", exc_info=True)
                raise

            self._is_running = True
        except Exception as e:
            self._logger.error(f"Setup failed: {e}", exc_info=True)
            # Ensure cleanup happens even if setup fails
            self.cleanup()
            raise

    def _restore_sigint_handler(self):
        if self._original_sigint_handler is not None:
            signal.signal(signal.SIGINT, self._original_sigint_handler)

        self._is_running = False
        self._shutdown_called = True

    def cleanup(self) -> None:
        """
        Clean up resources and ensure strategy shutdown.

        This method ensures strategy.shutdown() is called and the broker
        is disconnected, even in case of crashes or errors. It is safe to
        call multiple times.
        """
        if self._shutdown_called:
            return

        self._logger.info("Cleaning up strategy runner...")

        try:
            # Call strategy shutdown (always, even on errors)
            self._logger.debug("Calling strategy.shutdown()...")
            try:
                self._strategy.shutdown()
                self._logger.info("Strategy shutdown completed successfully")
            except Exception as e:
                self._logger.error(
                    f"Strategy shutdown failed (continuing cleanup): {e}", exc_info=True
                )

        finally:
            # Disconnect broker (always, even if shutdown fails)
            try:
                self._logger.debug("Disconnecting from broker...")
                self._broker.disconnect()
                self._logger.info("Broker disconnected")
            except Exception as e:
                self._logger.error(f"Broker disconnect failed: {e}", exc_info=True)

            # Restore original signal handler
            self._restore_sigint_handler()
            self._logger.info("Cleanup complete")

    async def cleanup_async(self):
        if self._shutdown_called:
            return

        self._logger.info("Cleaning up strategy runner...")

        try:
            # Call strategy shutdown (always, even on errors)
            self._logger.debug("Calling strategy.shutdown()...")
            try:
                self._strategy.shutdown()
                self._logger.info("Strategy shutdown completed successfully")
            except Exception as e:
                self._logger.error(
                    f"Strategy shutdown failed (continuing cleanup): {e}", exc_info=True
                )

        finally:
            # Disconnect broker (always, even if shutdown fails)
            try:
                self._logger.debug("Disconnecting from broker...")
                await self._broker.disconnect_async()
                self._logger.info("Broker disconnected")
            except Exception as e:
                self._logger.error(f"Broker disconnect failed: {e}", exc_info=True)

            # Restore original signal handler
            self._restore_sigint_handler()
            self._logger.info("Cleanup complete")

    def _signal_handler(self, signum, frame):
        """
        Handle interrupt signals for graceful shutdown.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        self._logger.warning("Received interrupt signal, shutting down gracefully...")
        self.cleanup()
        sys.exit(0)
