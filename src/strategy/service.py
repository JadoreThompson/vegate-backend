import logging
import os
from uuid import UUID

from sqlalchemy import select

from config import SRC_PATH
from enums import MarketType, Timeframe
from infra.db.model import StrategyDeployments, Strategy as StrategyEntity
from infra.db import get_db_sess_sync
from service.event.publisher.sync import SyncEventPublisherService
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
        event_publisher: SyncEventPublisherService,
    ):
        self._deployment_id = deployment_id
        self._ohlc_feed_client = ohlc_feed_client
        self._oms_client = oms_client
        self._strategy_config: StrategyConfig | None = None
        self._event_publisher = event_publisher
        self._fpath = os.path.join(SRC_PATH, "user_strategy.py")
        self._logger = logging.getLogger(self.__class__.__name__)

    def setup(self):
        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                select(StrategyDeployments, StrategyEntity)
                .join(
                    StrategyEntity,
                    StrategyDeployments.strategy_id == StrategyEntity.strategy_id,
                )
                .where(StrategyDeployments.deployment_id == self._deployment_id)
            )
            data: tuple[StrategyDeployments, StrategyEntity] = res.first()
            if data is None:
                raise ValueError(
                    f"Deployment with id '{self._deployment_id}' not found"
                )

        deployment, strategy = data
        if deployment is None:
            raise ValueError(f"Deployment with id '{self._deployment_id}' not found")

        strategy_config = StrategyConfig(
            symbol=deployment.symbol,
            market_type=MarketType(deployment.market_type),
            timeframe=Timeframe(deployment.timeframe),
        )
        self._write_code(strategy.code)
        self._strategy = self._load_strategy(strategy_config)

    def run(self) -> None:
        try:
            self._ohlc_feed_client.connect()
            self._oms_client.create_session(self._deployment_id)
            self._oms_client.connect()

            self._strategy.startup()
            for candle in self._ohlc_feed_client.candles():
                self._strategy.on_candle(candle)
        finally:
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
