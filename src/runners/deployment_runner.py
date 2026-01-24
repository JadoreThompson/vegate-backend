import asyncio
import json
import logging
import os
from uuid import UUID

from sqlalchemy import update

from config import BASE_PATH, REDIS_DEPLOYMENT_EVENTS_KEY
from core.enums import DeploymentStatus
from enums import BrokerType, DeploymentStatus
from infra.db.models import BrokerConnections, StrategyDeployments, Strategies
from lib.brokers import AlpacaBroker, ProxyBroker
from lib.strategy_manager import StrategyManager
from models import DeploymentConfig
from infra.db import get_db_sess_sync
from infra.redis import REDIS_CLIENT
from core.events import DeploymentEvent, DeploymentEventType
from utils import get_datetime
from services import EncryptionService
from services.brokers_apis.alpaca import AlpacaOAuthPayload
from user_strategy import Strategy
from .base import BaseRunner


class DeploymentRunner(BaseRunner):
    """Manages and runs a live strategy deployment."""

    def __init__(self, deployment_id: UUID):
        self._deployment_id = deployment_id
        self._deployment_config: DeploymentConfig| None = None
        self._strategy_manager: StrategyManager | None = None
        self._logger = logging.getLogger(type(self).__name__)

    def run(self):
        """The main entry point for the deployment process."""
        asyncio.run(self._run())

    async def _run(self) -> None:
        """The main entry point for the deployment process."""
        self._logger.info(f"Starting DeploymentRunner for ID '{self._deployment_id}'")

        db_deployment: StrategyDeployments | None = None
        db_strategy: Strategies | None = None
        db_broker_conn: BrokerConnections | None = None

        final_status: DeploymentStatus | None = None
        final_error_message: str | None = None

        try:
            # Fetch deployment, strategy, and broker connection from database
            db_deployment, db_strategy, db_broker_conn = (
                self._fetch_deployment_dependencies()
            )
            if db_deployment is None or db_strategy is None or db_broker_conn is None:
                return

            self._update_status(DeploymentStatus.RUNNING)

            # Write strategy code to file
            self._write_strategy_code(db_strategy.code)

            # Create deployment config
            self._deployment_config = DeploymentConfig(
                symbol=db_deployment.symbol,
                deployment_id=self._deployment_id,
                broker=BrokerType(db_broker_conn.broker),
            )

            # Initialize strategy manager with config
            # self._strategy_manager = StrategyManager(deployment_config)
            self._logger.info("Strategy manager initialised")

            strategy_task = asyncio.create_task(
                self._run_strategy_manager()
            )
            event_task = asyncio.create_task(self._listen_for_events())
            done, pending = await asyncio.wait(
                [strategy_task, event_task], return_when=asyncio.FIRST_COMPLETED
            )
            # print(done, pending)

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except KeyboardInterrupt:
            self._logger.info(f"Deployment {self._deployment_id} interrupted by user")
            final_status = DeploymentStatus.STOPPED
        except asyncio.CancelledError:
            self._logger.info(f"Deployment {self._deployment_id} cancelled")
            final_status = DeploymentStatus.STOPPED
        except Exception as e:
            self._logger.exception(
                f"Error during deployment execution for {self._deployment_id}"
            )
            final_status = DeploymentStatus.ERROR
            final_error_message = str(e)
        finally:
            final_status = final_status or DeploymentStatus.STOPPED
            self._update_status(final_status, final_error_message)
            self._logger.info(f"Deployment {self._deployment_id} finished")

    def _fetch_deployment_dependencies(
        self,
    ) -> tuple[StrategyDeployments | None, Strategies | None, BrokerConnections | None]:
        """Fetch deployment, strategy, and broker connection from database.

        Returns:
            Tuple of (db_deployment, db_strategy, db_broker_conn) or (None, None, None) if not found
        """
        with get_db_sess_sync() as db_sess:
            db_deployment = db_sess.get(StrategyDeployments, self._deployment_id)
            if db_deployment is None:
                self._logger.error(
                    f"Deployment not found for ID: {self._deployment_id}"
                )
                return None, None, None

            self._logger.info(
                f"Deployment found. Symbol: {db_deployment.symbol}, "
                f"Timeframe: {db_deployment.timeframe}"
            )

            db_strategy = db_sess.get(Strategies, db_deployment.strategy_id)
            if db_strategy is None:
                self._logger.error(
                    f"Strategy not found for deployment {self._deployment_id}: "
                    f"{db_deployment.strategy_id}"
                )
                self._update_status(
                    DeploymentStatus.ERROR,
                    error_message="Strategy not found",
                )
                return None, None, None

            self._logger.info(f"Strategy found: {db_strategy.name}")

            db_broker_conn = db_sess.get(
                BrokerConnections, db_deployment.broker_connection_id
            )
            if db_broker_conn is None:
                self._logger.error(
                    f"Broker connection not found for deployment {self._deployment_id}: "
                    f"{db_deployment.broker_connection_id}"
                )
                self._update_status(
                    DeploymentStatus.ERROR,
                    error_message="Broker connection not found",
                )
                return None, None, None

            self._logger.info(f"Broker connection found: {db_broker_conn.broker}")

            # Expunge objects from session to use outside of context
            db_sess.expunge(db_deployment)
            db_sess.expunge(db_strategy)
            db_sess.expunge(db_broker_conn)

        return db_deployment, db_strategy, db_broker_conn

    def _write_strategy_code(self, code: str) -> None:
        """Write strategy code to user_strategy.py file.

        Args:
            code: Strategy code to write
        """
        temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")
        with open(temp_strategy_path, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {temp_strategy_path}")

    async def _run_strategy_manager(self) -> None:
        """Run the strategy manager in a separate task."""
        try:
            self._logger.info("Strategy manager started, entering trading loop")

            # Instantiate broker
            broker = self._create_broker()
            self._logger.info(f"Broker instantiated: {type(broker).__name__}")

            # Wrap broker in proxy
            proxy_broker = ProxyBroker(self._deployment_id, broker)

            # Instantiate strategy
            strategy = Strategy(name=self._deployment_config.symbol, broker=proxy_broker)
            self._logger.info("Strategy instantiated")

            # Create strategy manager with pre-instantiated objects
            strategy_manager = StrategyManager(
                broker=broker, strategy=strategy, config=self._deployment_config
            )

            # Call run or run_async based on broker capabilities
            if broker.supports_async:
                self._logger.info("Broker supports async, calling run_async()")
                await strategy_manager.run_async()
            else:
                self._logger.info("Broker does not support async, calling run()")
                strategy_manager.run()

        except asyncio.CancelledError:
            self._logger.info(
                f"Strategy task for deployment {self._deployment_id} was cancelled"
            )
            raise
        except Exception as e:
            self._logger.exception(f"Error in strategy execution: {e}")
            raise

    async def _listen_for_events(self) -> None:
        """Listen for deployment events on Redis pub/sub."""
        try:
            async with REDIS_CLIENT.pubsub() as ps:
                await ps.subscribe(REDIS_DEPLOYMENT_EVENTS_KEY)
                self._logger.info(
                    f"Subscribed to deployment events channel: {REDIS_DEPLOYMENT_EVENTS_KEY}"
                )

                async for message in ps.listen():
                    if message["type"] == "message":
                        try:
                            event_data = json.loads(message["data"])
                            event = DeploymentEvent(**event_data)

                            # Only handle events for this deployment
                            if event.deployment_id != self._deployment_id:
                                continue

                            self._logger.info(
                                f"Received event: {event.type} for deployment {event.deployment_id}"
                            )

                            if event.type == DeploymentEventType.STOP:
                                self._logger.info(
                                    f"Stop event received for deployment {self._deployment_id}"
                                )
                                break

                        except (json.JSONDecodeError, ValueError) as e:
                            self._logger.error(f"Failed to parse deployment event: {e}")
                            continue

        except asyncio.CancelledError:
            self._logger.info("Event listener cancelled")
            raise
        except Exception as e:
            self._logger.exception(f"Error in event listener: {e}")
            raise

    def _update_status(
        self,
        status: DeploymentStatus,
        error_message: str | None = None,
    ) -> None:
        """Update deployment status in database.

        Args:
            status: New deployment status
            error_message: Optional error message if status is ERROR
        """
        try:
            with get_db_sess_sync() as db_sess:
                values = {"status": status.value}
                if error_message is not None:
                    values["error_message"] = error_message
                if status == DeploymentStatus.STOPPED:
                    values["stopped_at"] = get_datetime()

                db_sess.execute(
                    update(StrategyDeployments)
                    .where(StrategyDeployments.deployment_id == self._deployment_id)
                    .values(**values)
                )
                db_sess.commit()

                self._logger.info(
                    f"Updated deployment {self._deployment_id} status to {status.value}"
                )
        except Exception as e:
            self._logger.exception(f"Failed to update deployment status: {e}")

    def _create_broker(self):
        """Create a broker instance based on the deployment config.

        Returns:
            BaseBroker instance

        Raises:
            ValueError: If broker type is not supported
        """
        if self._deployment_config.broker == BrokerType.ALPACA:
            return self._create_alpaca_broker_v2()
        else:
            raise ValueError(
                f"Unsupported broker type: {self._deployment_config.broker}"
            )

    def _create_alpaca_broker(self) -> AlpacaBroker:
        """Create an Alpaca broker with OAuth credentials from database.

        Returns:
            AlpacaBroker instance

        Raises:
            ValueError: If broker connection or credentials not found
        """
        with get_db_sess_sync() as db_sess:
            # Fetch the deployment to get broker connection ID
            deployment = db_sess.get(StrategyDeployments, self._deployment_id)
            if not deployment:
                raise ValueError(f"Deployment not found: {self._deployment_id}")

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
            )

    def _create_alpaca_broker_v2(self):
        with get_db_sess_sync() as db_sess:
            # Fetch the deployment to get broker connection ID
            deployment = db_sess.get(StrategyDeployments, self._deployment_id)
            if not deployment:
                raise ValueError(f"Deployment not found: {self._deployment_id}")

            # Fetch the broker connection
            broker_conn = db_sess.get(
                BrokerConnections, deployment.broker_connection_id
            )
            
            api_key = broker_conn.api_key
            secret_key = broker_conn.secret_key

        return AlpacaBroker(api_key=api_key, secret_key=secret_key)