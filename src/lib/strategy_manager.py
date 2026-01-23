import asyncio
import json

from enums import BrokerType
from lib.brokers import AlpacaBroker, BaseBroker, ProxyBroker
from lib.strategy import BaseStrategy
from models import DeploymentConfig

from infra.db.models import StrategyDeployments, BrokerConnections
from events.strategy import StrategyError
from infra.kafka import KafkaProducer
from services import EncryptionService
from services.brokers_apis.alpaca import AlpacaOAuthPayload
from user_strategy import Strategy
from infra.db import get_db_sess_sync


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
        proxy_broker = ProxyBroker(self._config.deployment_id, broker)

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
        event = StrategyError(deployment_id=self._config.deployment_id, error_msg=error_msg)
        self.producer.send("strategies", json.dumps(event.model_dump()).encode())

    def _create_broker(self) -> BaseBroker:
        """Create a broker instance based on the deployment config.

        Returns:
            BaseBroker instance

        Raises:
            ValueError: If broker type is not supported
        """
        if self._config.broker == BrokerType.ALPACA:
            return self._create_alpaca_broker()
        else:
            raise ValueError(f"Unsupported broker type: {self._config.broker}")

    def _create_alpaca_broker(self) -> AlpacaBroker:
        """Create an Alpaca broker with OAuth credentials from database.

        Returns:
            AlpacaBroker instance

        Raises:
            ValueError: If broker connection or credentials not found
        """
        with get_db_sess_sync() as db_sess:
            # Fetch the deployment to get broker connection ID
            deployment = db_sess.get(StrategyDeployments, self._config.deployment_id)
            if not deployment:
                raise ValueError(
                    f"Deployment not found: {self._config.deployment_id}"
                )

            # Fetch the broker connection
            broker_conn = db_sess.get(
                BrokerConnections, deployment.broker_connection_id
            )
            if not broker_conn:
                raise ValueError(
                    f"Broker connection not found: {deployment.broker_connection_id}"
                )

            # Decrypt OAuth payload
            decrypted = EncryptionService.decrypt(
                broker_conn.oauth_payload, str(broker_conn.user_id)
            )
            oauth_payload = AlpacaOAuthPayload(**json.loads(decrypted))

            # Create and return AlpacaBroker
            return AlpacaBroker(
                oauth_token=oauth_payload.access_token,
                paper=oauth_payload.env == "paper",
                is_crypto=False,  # Can be extended to support crypto
            )
