import logging
import os
import time
from threading import Thread
from uuid import UUID

from redis import Redis
from sqlalchemy import select

from config import (
    REDIS_STRATEGY_HEARTBEAT_KEY_PREFIX,
    SRC_PATH,
    STRATEGY_DEPLOYMENT_EVENTS_KEY,
)
from enums import BrokerType, MarketType, StrategyDeploymentStatus, Timeframe
from events.deployment import DeploymentStatusChangedEvent
from infra.db.model import StrategyDeployments, Strategy as StrategyEntity
from infra.db import get_db_sess_sync
from infra.db.model.instrument import Instrument
from service.event.publisher import SyncEventPublisher
from service.ohlc.feed.client import OHLCFeedClient
from service.oms.client import OMSClient
from strategy.model import StrategyConfig


class StrategyDeploymentService:
    """Manages and runs a live strategy deployment."""

    def __init__(
        self,
        deployment_id: UUID,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: OMSClient,
        event_publisher: SyncEventPublisher,
        redis_client: Redis,
        heartbeat_interval: int = 5,
        heartbeat_key_prefix: str = REDIS_STRATEGY_HEARTBEAT_KEY_PREFIX,
    ):
        self._deployment_id = deployment_id
        self._ohlc_feed_client = ohlc_feed_client
        self._oms_client = oms_client
        self._strategy_config: StrategyConfig | None = None
        self._event_publisher = event_publisher
        self._redis_client = redis_client
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_key_prefix = heartbeat_key_prefix

        self._alive = False
        self._heartbeat_th: Thread | None = None
        self._fpath = os.path.join(SRC_PATH, "user_strategy.py")
        self._logger = logging.getLogger(self.__class__.__name__)

    def setup(self):
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(StrategyDeployments, StrategyEntity, Instrument)
                .join(Instrument, Instrument.id == StrategyDeployments.instrument_id)
                .join(
                    StrategyEntity,
                    StrategyDeployments.strategy_id == StrategyEntity.strategy_id,
                )
                .where(StrategyDeployments.deployment_id == self._deployment_id)
            )
            data: tuple[StrategyDeployments, StrategyEntity, Instrument] = res.first()
            if data is None:
                raise ValueError(
                    f"Deployment with id '{self._deployment_id}' not found"
                )

        deployment, strategy, instrument = data
        if deployment is None:
            raise ValueError(f"Deployment with id '{self._deployment_id}' not found")

        strategy_config = StrategyConfig(
            symbol=instrument.native_symbol,
            market_type=MarketType(instrument.market_type),
            broker_type=BrokerType(instrument.broker_type),
            timeframe=Timeframe(deployment.timeframe),
        )
        self._write_code(strategy.code)
        self._strategy = self._load_strategy(strategy_config)

    def run(self) -> None:
        try:
            self._alive = True
            self._event_publisher.enqueue(
                DeploymentStatusChangedEvent(
                    deployment_id=self._deployment_id,
                    status=StrategyDeploymentStatus.RUNNING,
                ),
                STRATEGY_DEPLOYMENT_EVENTS_KEY,
            )
            self._heartbeat_th = Thread(
                target=self._heartbeat_loop, name="HeartbeatLoop"
            )
            self._heartbeat_th.start()

            self._ohlc_feed_client.connect()
            self._oms_client.create_session(self._deployment_id)
            self._oms_client.connect()

            self._strategy.startup()
            for candle in self._ohlc_feed_client.candles():
                self._strategy.on_candle(candle)
        except KeyboardInterrupt:
            pass
        finally:
            self._alive = False
            self._event_publisher.enqueue(
                DeploymentStatusChangedEvent(
                    deployment_id=self._deployment_id,
                    status=StrategyDeploymentStatus.STOPPED,
                ),
                STRATEGY_DEPLOYMENT_EVENTS_KEY,
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

    def _load_strategy(self, config: StrategyConfig):
        if not os.path.exists(self._fpath):
            raise FileNotFoundError(f"File not found at '{self._fpath}'")

        from user_strategy import UserStrategy

        strategy = UserStrategy(
            config=config,
            ohlc_feed_client=self._ohlc_feed_client,
            oms_client=self._oms_client,
            event_publisher=self._event_publisher,
        )

        return strategy

    def _heartbeat_loop(self):
        while self._alive:
            time.sleep(self._heartbeat_interval)
            if not self._alive:
                break
            
            self._logger.info("Setting heartbeat...")
            self._redis_client.set(
                f"{self._heartbeat_key_prefix}{self._deployment_id}", 1, ex=15
            )
