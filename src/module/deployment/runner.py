import logging
import os
import time
from threading import Thread
from uuid import UUID

from redis import Redis
from sqlalchemy import select

from config import HISTORICAL_BASE_URL, REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX, SRC_PATH
from core.db import get_db_sess_sync
from module.deployment.enums import StrategyDeploymentStatus

from module.event_bus import SyncEventPublisher
from module.markets.feed import OHLCFeedClient
from module.markets.historical import HistoricalDataClient
from module.strategy.model import Strategy, StrategyVersion
from module.strategy.strategy import BaseStrategy
from .event import DeploymentStatusChangedEvent
from .model import StrategyDeployments
from .oms import OMSClient


class StrategyDeploymentRunner:
    """Manages and runs a live strategy deployment."""

    def __init__(
        self,
        deployment_id: UUID,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: OMSClient,
        event_publisher: SyncEventPublisher,
        redis_client: Redis,
        heartbeat_interval: int = 5,
        heartbeat_key_prefix: str = REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX,
    ):
        self._deployment_id = deployment_id
        self._ohlc_feed_client = ohlc_feed_client
        self._oms_client = oms_client
        self._event_publisher = event_publisher
        self._redis_client = redis_client
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_key_prefix = heartbeat_key_prefix

        self._alive = False
        self._heartbeat_th: Thread | None = None
        self._strategy: BaseStrategy | None = None
        self._fpath = os.path.join(SRC_PATH, "user_strategy.py")
        self._logger = logging.getLogger(self.__class__.__name__)

    def setup(self):
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(StrategyDeployments, StrategyVersion)
                .join(StrategyVersion, StrategyVersion.id == StrategyDeployments.version_id)
                .where(StrategyDeployments.deployment_id == self._deployment_id)
            )

            data: tuple[StrategyDeployments, StrategyVersion] = res.first()
            if data is None:
                raise ValueError(
                    f"Deployment with id '{self._deployment_id}' not found"
                )

        deployment, strategy_version = data
        if deployment is None:
            raise ValueError(f"Deployment with id '{self._deployment_id}' not found")

        if deployment.status not in {
            StrategyDeploymentStatus.STOPPED,
            StrategyDeploymentStatus.PENDING,
        }:
            raise ValueError(
                f"Deployment with id '{self._deployment_id}' is not stopped. Aborting deployment"
            )

        self._write_code(strategy_version.code)
        self._strategy = self._load_strategy()

    def run(self) -> None:
        try:
            self.setup()
            self._alive = True
            self._event_publisher.enqueue(
                DeploymentStatusChangedEvent(
                    deployment_id=self._deployment_id,
                    status=StrategyDeploymentStatus.RUNNING,
                )
            )
            self._heartbeat_th = Thread(
                target=self._heartbeat_loop, name="HeartbeatLoop"
            )
            self._heartbeat_th.start()

            self._ohlc_feed_client.connect()
            self._oms_client.create_session(self._deployment_id)

            self._strategy.startup()
            for candle in self._ohlc_feed_client.candles():
                if not self._alive:
                    break
                self._strategy.on_candle(candle)
        except KeyboardInterrupt:
            pass
        finally:
            self._alive = False
            self._event_publisher.enqueue(
                DeploymentStatusChangedEvent(
                    deployment_id=self._deployment_id,
                    status=StrategyDeploymentStatus.STOPPED,
                )
            )

            if self._heartbeat_th is not None and self._heartbeat_th.is_alive():
                try:
                    self._heartbeat_th.join(timeout=self._heartbeat_interval + 1)
                except TimeoutError:
                    self._logger.info("Heartbeat thread failed to stop")

            self._strategy.shutdown()
            self._ohlc_feed_client.close()
            self._oms_client.disconnect()

    def _write_code(self, code: str):
        with open(self._fpath, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {self._fpath}")

    def _load_strategy(self) -> BaseStrategy:
        if not os.path.exists(self._fpath):
            raise FileNotFoundError(f"File not found at '{self._fpath}'")

        from user_strategy import UserStrategy

        historical_data_client = HistoricalDataClient(base_url=HISTORICAL_BASE_URL)

        strategy = UserStrategy(
            ohlc_feed_client=self._ohlc_feed_client,
            oms_client=self._oms_client,
            event_publisher=self._event_publisher,
            historical_data_client=historical_data_client,
        )

        return strategy

    def _heartbeat_loop(self):
        try:
            while self._alive:
                time.sleep(self._heartbeat_interval)
                if not self._alive:
                    break

                self._logger.info("Setting heartbeat...")
                self._redis_client.set(
                    f"{self._heartbeat_key_prefix}{self._deployment_id}", 1, ex=15
                )
        finally:
            self._alive = False