import asyncio
import json
import logging
import os
from uuid import UUID

from sqlalchemy import update

from config import BASE_PATH
from core.enums import StrategyDeploymentStatus
from db_models import BrokerConnections, StrategyDeployments, Strategies
from engine.brokers import BaseBroker, AlpacaBroker
from engine.enums import BrokerType, MarketType, Timeframe
from engine.strategy import StrategyContext, StrategyManager
from services import EncryptionService
from services.brokers.alpaca import AlpacaOAuthPayload
from utils.db import get_db_sess_sync
from .base_runner import BaseRunner


logger = logging.getLogger(__name__)


class DeploymentRunner(BaseRunner):
    """
    Manages and runs a live strategy deployment.
    """

    def __init__(self, deployment_id: str | UUID):
        self.deployment_id = UUID(str(deployment_id))
        self._strategy_manager: StrategyManager | None = None
        self._broker: BaseBroker | None = None

    def run(self):
        asyncio.run(self._run())

    async def _run(self) -> None:
        """The main entry point for the deployment process."""
        logger.info(f"Starting DeploymentRunner for ID '{self.deployment_id}'")

        db_deployment = None
        db_strategy = None
        db_broker_conn = None

        try:
            with get_db_sess_sync() as db_sess:
                db_deployment = db_sess.get(StrategyDeployments, self.deployment_id)
                if db_deployment is None:
                    logger.error(f"Deployment not found for ID: {self.deployment_id}")
                    return

                logger.info(
                    f"Deployment found. Ticker: {db_deployment.symbol}, "
                    f"Timeframe: {db_deployment.timeframe}"
                )

                db_strategy = db_sess.get(Strategies, db_deployment.strategy_id)
                if db_strategy is None:
                    logger.error(
                        f"Strategy not found for deployment {self.deployment_id}: "
                        f"{db_deployment.strategy_id}"
                    )
                    self._update_status(
                        StrategyDeploymentStatus.ERROR,
                        error_message="Strategy not found",
                    )
                    return

                logger.info(f"Strategy found: {db_strategy.name}")

                db_broker_conn = db_sess.get(
                    BrokerConnections, db_deployment.broker_connection_id
                )
                if db_broker_conn is None:
                    logger.error(
                        f"Broker connection not found for deployment {self.deployment_id}: "
                        f"{db_deployment.broker_connection_id}"
                    )
                    self._update_status(
                        StrategyDeploymentStatus.ERROR,
                        error_message="Broker connection not found",
                    )
                    return

                logger.info(f"Broker connection found: {db_broker_conn.broker}")

                db_sess.expunge(db_deployment)
                db_sess.expunge(db_strategy)
                db_sess.expunge(db_broker_conn)

            self._update_status(StrategyDeploymentStatus.RUNNING)

            temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")
            with open(temp_strategy_path, "w") as f:
                f.write(db_strategy.code)
            logger.info(f"Strategy code written to {temp_strategy_path}")

            from user_strategy import Strategy  # type: ignore

            strategy = Strategy()
            logger.info("Strategy instance created")

            self._broker = self._initialise_broker(
                db_broker_conn, db_deployment.market_type
            )
            logger.info("Broker initialised")

            self._strategy_manager = StrategyManager(strategy, self._broker)
            logger.info("Strategy manager initialised")

            context = StrategyContext(self._broker)
            timeframe = Timeframe(db_deployment.timeframe)

            with self._strategy_manager:
                logger.info("Strategy manager started, entering trading loop")

                async for candle in self._broker.yield_ohlcv_async(
                    symbol=db_deployment.symbol, timeframe=timeframe
                ):
                    logger.info(f"Got candle {candle}")
                    context._current_candle = candle
                    self._strategy_manager.on_candle(context)

        except KeyboardInterrupt:
            logger.info(f"Deployment {self.deployment_id} interrupted by user")
            self._update_status(StrategyDeploymentStatus.STOPPED)
        except Exception as e:
            logger.exception(
                f"Error during deployment execution for {self.deployment_id}"
            )
            self._update_status(StrategyDeploymentStatus.ERROR, error_message=str(e))
        finally:
            logger.info(f"Deployment {self.deployment_id} finished")

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
                    .where(StrategyDeployments.deployment_id == self.deployment_id)
                    .values(**values)
                )
                db_sess.commit()
                logger.info(
                    f"Updated deployment {self.deployment_id} status to {status.value}"
                )
        except Exception as e:
            logger.exception(f"Failed to update deployment status: {e}")
