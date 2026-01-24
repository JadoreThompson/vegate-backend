import logging

from redis import Redis

from config import REDIS_DEPLOYMENT_EVENTS_KEY
from enums import Timeframe
from events.deployment import DeploymentErrorEvent
from infra.redis import REDIS_CLIENT_SYNC
from lib.brokers import BaseBroker
from lib.strategy import BaseStrategy
from models import DeploymentConfig


class StrategyManager:
    """Manages strategy execution with pre-instantiated broker and strategy.

    The manager handles the lifecycle of strategy execution, calling startup(),
    feeding candles via on_candle(), and calling shutdown().
    """

    def __init__(
        self,
        broker: BaseBroker,
        strategy: BaseStrategy,
        config: DeploymentConfig,
        redis_client: Redis = REDIS_CLIENT_SYNC,
    ):
        """Initialize the strategy manager.

        Args:
            broker: Pre-instantiated broker instance
            strategy: Pre-instantiated strategy instance
            config: DeploymentConfig object containing strategy configuration
            redis_client: Redis client for event publishing
        """
        self._broker = broker
        self._strategy = strategy
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self.redis_client = redis_client

    def run(self):
        """Run the strategy synchronously with the configured broker."""
        # Call startup hook
        self._strategy.startup()

        try:
            # Stream candles synchronously and feed to strategy
            for candle in self._broker.stream_candles(
                self._config.symbol, Timeframe.m1
            ):
                self._strategy.on_candle(candle)
        except Exception as e:
            self._emit_strategy_error(str(e))
            raise
        finally:
            # Call shutdown hook
            self._strategy.shutdown()

    async def run_async(self):
        """Run the strategy asynchronously with the configured broker."""
        # Call startup hook
        self._strategy.startup()

        try:
            # Stream candles asynchronously and feed to strategy
            async for candle in self._broker.stream_candles_async(
                self._config.symbol, Timeframe.m1
            ):
                self._strategy.on_candle(candle)
        except Exception as e:
            self._emit_strategy_error(str(e))
            raise
        finally:
            # Call shutdown hook
            self._strategy.shutdown()

    def _emit_strategy_error(self, error_msg: str):
        """Emit a strategy error event.

        Args:
            error_msg: Error message
        """
        event = DeploymentErrorEvent(
            deployment_id=self._config.deployment_id, error_msg=error_msg
        )
        self.redis_client.publish(REDIS_DEPLOYMENT_EVENTS_KEY, event.model_dump_json())
