import asyncio
import json
import logging
import os
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import update

from config import BASE_PATH, REDIS_DEPLOYMENT_EVENTS_KEY
from core.enums import StrategyDeploymentStatus
from core.events import DeploymentEvent, DeploymentEventType
from db_models import BrokerConnections, StrategyDeployments, Strategies
from engine.brokers import AlpacaBroker, BaseBroker
from engine.enums import BrokerType, MarketType, Timeframe
from engine.strategy import StrategyContext, StrategyManager
from services import EncryptionService
from services.brokers_apis.alpaca import AlpacaOAuthPayload
from utils.db import get_db_sess_sync
from utils.redis import REDIS_CLIENT
from .base import BaseRunner




class DeploymentRunner(BaseRunner):
    """
    Manages and runs a live strategy deployment.
    """

    def __init__(self, deployment_id: str | UUID):
        self._deployment_id = UUID(str(deployment_id))
        self._strategy_manager: StrategyManager | None = None
        self._broker: BaseBroker | None = None
        self._strategy_task: asyncio.Task | None = None
        self._logger = logging.getLogger(type(self).__name__)

    def run(self):
        asyncio.run(self._run())

    async def _run(self) -> None:
        """The main entry point for the deployment process."""
        self._logger.info(f"Starting DeploymentRunner for ID '{self._deployment_id}'")

        db_deployment = None
        db_strategy = None
        db_broker_conn = None

        final_status = None
        final_error_message = None

        try:
            with get_db_sess_sync() as db_sess:
                db_deployment = db_sess.get(StrategyDeployments, self._deployment_id)
                if db_deployment is None:
                    self._logger.error(f"Deployment not found for ID: {self._deployment_id}")
                    return

                self._logger.info(
                    f"Deployment found. Ticker: {db_deployment.symbol}, "
                    f"Timeframe: {db_deployment.timeframe}"
                )

                db_strategy = db_sess.get(Strategies, db_deployment.strategy_id)
                if db_strategy is None:
                    self._logger.error(
                        f"Strategy not found for deployment {self._deployment_id}: "
                        f"{db_deployment.strategy_id}"
                    )
                    self._update_status(
                        StrategyDeploymentStatus.ERROR,
                        error_message="Strategy not found",
                    )
                    return

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
                        StrategyDeploymentStatus.ERROR,
                        error_message="Broker connection not found",
                    )
                    return

                self._logger.info(f"Broker connection found: {db_broker_conn.broker}")

                db_sess.expunge(db_deployment)
                db_sess.expunge(db_strategy)
                db_sess.expunge(db_broker_conn)

            self._update_status(StrategyDeploymentStatus.RUNNING)

            temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")
            with open(temp_strategy_path, "w") as f:
                f.write(db_strategy.code)
            self._logger.info(f"Strategy code written to {temp_strategy_path}")

            from user_strategy import Strategy  # type: ignore

            strategy = Strategy()
            self._logger.info("Strategy instance created")

            self._broker = self._initialise_broker(db_broker_conn, db_deployment.market_type)
            self._logger.info("Broker initialised")

            self._strategy_manager = StrategyManager(strategy, self._broker)
            self._logger.info("Strategy manager initialised")

            context = StrategyContext(self._broker)
            timeframe = Timeframe(db_deployment.timeframe)

            # Create strategy task
            self._strategy_task = asyncio.create_task(
                self._run_strategy(db_deployment.symbol, timeframe, context)
            )

            # Start listening for deployment events
            await self._listen_for_events()

        except KeyboardInterrupt:
            self._logger.info(f"Deployment {self._deployment_id} interrupted by user")
            final_status = StrategyDeploymentStatus.STOPPED
            if self._strategy_task and not self._strategy_task.done():
                await self._cancel_strategy_task()
        except asyncio.CancelledError:
            self._logger.info(f"Deployment {self._deployment_id} cancelled")
            final_status = StrategyDeploymentStatus.STOPPED
        except Exception as e:
            self._logger.exception(
                f"Error during deployment execution for {self._deployment_id}"
            )
            final_status = StrategyDeploymentStatus.ERROR
            final_error_message = str(e)
            if self._strategy_task and not self._strategy_task.done():
                await self._cancel_strategy_task()
        finally:
            final_status = final_status or StrategyDeploymentStatus.STOPPED
            self._update_status(final_status, final_error_message)
            self._logger.info(f"Deployment {self._deployment_id} finished")

    async def _run_strategy(
        self, symbol: str, timeframe: Timeframe, context: StrategyContext
    ) -> None:
        """
        Run the strategy trading loop.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            context: Strategy context
        """

        try:
            self._strategy_manager.setup()

            self._logger.info("Strategy manager started, entering trading loop")

            async for candle in self._broker.yield_ohlcv_async(
                symbol=symbol, timeframe=timeframe
            ):
                self._logger.info(f"Got candle {candle}")
                context._current_candle = candle
                self._strategy_manager.on_candle(context)

        except asyncio.CancelledError:
            self._logger.info(
                f"Strategy task for deployment {self._deployment_id} was cancelled"
            )
            raise
        except Exception as e:
            self._logger.exception(f"Error in strategy execution: {e}")
            raise
        finally:
            if self._strategy_manager.supports_async:
                await self._strategy_manager.cleanup_async()
            else:
                self._strategy_manager.cleanup()
    
    async def _listen_for_events(self) -> None:
        """
        Listen for deployment events on Redis pub/sub.
        """
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
                                await self._cancel_strategy_task()
                                break

                        except (json.JSONDecodeError, ValidationError) as e:
                            self._logger.error(f"Failed to parse deployment event: {e}")
                            continue

        except asyncio.CancelledError:
            self._logger.info("Event listener cancelled")
            raise
        except Exception as e:
            self._logger.exception(f"Error in event listener: {e}")
            raise

    async def _cancel_strategy_task(self) -> None:
        """
        Gracefully cancel the strategy task.
        """
        if self._strategy_task and not self._strategy_task.done():
            self._logger.info(
                f"Cancelling strategy task for deployment {self._deployment_id}"
            )
            self._strategy_task.cancel()
            try:
                await self._strategy_task
            except asyncio.CancelledError:
                self._logger.info(f"Strategy task cancelled successfully")

    def _initialise_broker(
        self, broker_conn: BrokerConnections, market_type: MarketType
    ) -> AlpacaBroker:
        """
        Initialize the broker client with OAuth credentials.

        Args:
            broker_conn: Broker connection with OAuth payload or API key

        Returns:
            Initialized broker instance
        """

        if broker_conn.broker == BrokerType.ALPACA:
            return self._initialise_alpaca_broker(broker_conn, market_type)
        else:
            raise ValueError(f"Unsupported broker: {broker_conn.broker}")

    def _initialise_alpaca_broker(
        self, broker_conn: BrokerConnections, market_type: MarketType
    ):
        decrypted = EncryptionService.decrypt(
            broker_conn.oauth_payload, str(broker_conn.user_id)
        )
        oauth_payload = AlpacaOAuthPayload(**json.loads(decrypted))
        broker = AlpacaBroker(
            self._deployment_id,
            oauth_token=oauth_payload.access_token,
            is_crypto=market_type == MarketType.CRYPTO,
            paper=oauth_payload.env == "paper",
        )

        return broker

    def _update_status(
        self,
        status: StrategyDeploymentStatus,
        error_message: str | None = None,
    ) -> None:
        """
        Update deployment status in database.

        Args:
            status: New deployment status
            error_message: Optional error message if status is ERROR
        """
        try:
            with get_db_sess_sync() as db_sess:
                values = {"status": status.value}
                if error_message is not None:
                    values["error_message"] = error_message
                if status == StrategyDeploymentStatus.STOPPED:
                    from utils.utils import get_datetime

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
