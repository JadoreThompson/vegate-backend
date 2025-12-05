import logging
import signal
import sys

from engine.brokers import BaseBroker
from .base import BaseStrategy
from .config import StrategyConfig
from .context import StrategyContext


class StrategyRunner:
    def __init__(self, strategy: BaseStrategy, broker: BaseBroker):
        self._broker = broker
        self._strategy = strategy
        # self._config = config

        self._is_running = False
        self._shutdown_called = False
        self._original_sigint_handler = None
        self._logger = logging.getLogger(__name__)

    def __enter__(self):
        self._setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup()

    def process(self, context: StrategyContext) -> None:
        self._strategy.process(context)

    def _setup(self) -> None:
        """
        Set up the execution environment.

        This method:
        1. Registers signal handlers for graceful shutdown
        2. Connects to the broker
        3. Calls strategy.startup()

        Raises:
            Exception: If setup fails
        """
        self._logger.info("Setting up strategy runner...")

        try:
            # Register signal handler for graceful shutdown on Ctrl+C
            self._original_sigint_handler = signal.signal(
                signal.SIGINT, self._signal_handler
            )

            # Connect to broker
            self._logger.debug("Connecting to broker...")
            self._broker.connect()

            # Call strategy startup
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
            self._cleanup()
            raise

    def _cleanup(self) -> None:
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
            if self._original_sigint_handler is not None:
                signal.signal(signal.SIGINT, self._original_sigint_handler)

            self._is_running = False
            self._shutdown_called = True
            self._logger.info("Cleanup complete")

    def _signal_handler(self, signum, frame):
        """
        Handle interrupt signals for graceful shutdown.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        self._logger.warning("Received interrupt signal, shutting down gracefully...")
        self._cleanup()
        sys.exit(0)

    # def _group_bars_by_timestamp(
    #     self,
    #     bars: List[OHLCBar],
    # ) -> Dict[datetime, List[OHLCBar]]:
    #     """
    #     Group bars by timestamp for event-driven processing.

    #     Args:
    #         bars: List of OHLC bars

    #     Returns:
    #         Dictionary mapping timestamp to list of bars at that time
    #     """
    #     from collections import defaultdict

    #     bars_by_time = defaultdict(list)
    #     for bar in bars:
    #         bars_by_time[bar.timestamp].append(bar)
    #     return bars_by_time

    # def _process_tick(self, timestamp: datetime, bars: List[OHLCBar]) -> None:
    #     """
    #     Process a single tick (timestamp with bars for all symbols).

    #     This method:
    #     1. Updates broker state with current time and prices
    #     2. Updates current bars cache
    #     3. Creates a StrategyContext
    #     4. Calls strategy.run(context)

    #     Args:
    #         timestamp: Current timestamp
    #         bars: List of bars for all symbols at this timestamp

    #     Raises:
    #         Exception: If tick processing fails critically
    #     """
    #     # Update broker state (for simulated broker)
    #     if hasattr(self._broker, "set_current_time"):
    #         self._broker.set_current_time(timestamp)

    #     # Update current prices and bars
    #     for bar in bars:
    #         if hasattr(self._broker, "set_current_price"):
    #             self._broker.set_current_price(bar.symbol, bar.close)
    #         self._current_bars[bar.symbol] = bar

    #     # Create strategy context
    #     context = StrategyContext(
    #         timestamp=timestamp,
    #         bars=self._current_bars.copy(),
    #         broker=self._broker,
    #         data_loader=self.data_loader,
    #         timeframe=self.timeframe,
    #     )

    #     # Execute strategy
    #     try:
    #         self._strategy.run(context)
    #         self._bars_processed += len(bars)

    #     except KeyboardInterrupt:
    #         # Re-raise keyboard interrupt to allow graceful shutdown
    #         raise

    #     except Exception as e:
    #         # Log strategy errors but continue execution
    #         self._logger.error(
    #             f"Strategy execution error at {timestamp}: {e}",
    #             exc_info=True,
    #         )
    #         # Depending on requirements, you might want to:
    #         # - Continue (current behavior)
    #         # - Stop execution: raise
    #         # - Count errors and stop after threshold
    #         # For now, we continue to match backtesting engine behavior
