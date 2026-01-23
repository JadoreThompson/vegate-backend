import asyncio
import json

from enums import BrokerType
from lib.brokers import AlpacaBroker, BaseBroker, ProxyBroker
from lib.strategy import BaseStrategy
from models import DeploymentConfig

from user_strategy import Strategy
from infra.kafka import KafkaProducer
from events.strategy import StrategyError


class StrategyManager:
    """Manages strategy execution with broker configuration."""

    def __init__(self, config: DeploymentConfig):
        """Initialize the strategy manager.

        Args:
            config: DeploymentConfig object containing strategy configuration
        """
        self._config = config
        self.producer = KafkaProducer()

    def run(self):
        """Run the strategy with the configured broker."""
        # Instantiate the corresponding broker based on config
        broker = self._create_broker()

        # Wrap the broker in a proxy
        proxy_broker = ProxyBroker(self._config.strategy_id, broker)

        # Instantiate the strategy and pass the proxy broker
        strategy: BaseStrategy = Strategy(name=self._config.symbol, broker=proxy_broker)

        # Call startup hook
        strategy.startup()

        # Stream candles and feed to strategy
        if proxy_broker.supports_async:
            self._run_async(strategy, proxy_broker)
        else:
            self._run_sync(strategy, proxy_broker)

        # Call shutdown hook
        strategy.shutdown()

    def _run_sync(self, strategy: BaseStrategy, broker: ProxyBroker):
        """Run strategy with synchronous candle streaming.

        Args:
            strategy: Strategy instance
            broker: Proxy broker instance
        """
        for candle in broker.stream_candles(self._config.symbol, "1m"):
            try:
                strategy.on_candle(candle)
            except Exception as e:
                self._emit_strategy_error(str(e))

    def _run_async(self, strategy: BaseStrategy, broker: ProxyBroker):
        """Run strategy with asynchronous candle streaming.

        Args:
            strategy: Strategy instance
            broker: Proxy broker instance
        """
        asyncio.run(self._stream_candles_async(strategy, broker))

    async def _stream_candles_async(self, strategy: BaseStrategy, broker: ProxyBroker):
        """Stream candles asynchronously and feed to strategy.

        Args:
            strategy: Strategy instance
            broker: Proxy broker instance
        """
        async for candle in broker.stream_candles_async(self._config.symbol, "1m"):
            try:
                strategy.on_candle(candle)
            except Exception as e:
                self._emit_strategy_error(str(e))

    def _emit_strategy_error(self, error_msg: str):
        """Emit a strategy error event.

        Args:
            error_msg: Error message
        """
        event = StrategyError(strategy_id=self._config.strategy_id, error_msg=error_msg)
        self.producer.send("strategies", json.dumps(event.model_dump()).encode())

    def _create_broker(self) -> BaseBroker:
        """Create a broker instance based on the deployment config.

        Returns:
            BaseBroker instance

        Raises:
            ValueError: If broker type is not supported
        """
        if self._config.broker == BrokerType.ALPACA:
            return AlpacaBroker()
        else:
            raise ValueError(f"Unsupported broker type: {self._config.broker}")
