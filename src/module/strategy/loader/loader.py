import logging
import os

from config import SRC_PATH
from vegate.markets.feed.client import OHLCFeedClient
from vegate.markets.historical.client import HistoricalDataClient
from vegate.oms.client import OMSClient
from vegate.strategy.base import BaseStrategy
from .exception import StrategyLoadError


class StrategyLoader:

    def __init__(
        self,
        ohlc_feed_client: OHLCFeedClient,
        oms_client: OMSClient,
        historical_data_client: HistoricalDataClient,
    ):
        self._ohlc_feed_client = ohlc_feed_client
        self._oms_client = oms_client
        self._historical_data_client = historical_data_client
        self._fpath = os.path.join(SRC_PATH, "user_strategy.py")
        self._logger = logging.getLogger(type(self).__name__)

    def load_strategy(self, code: str) -> BaseStrategy:
        with open(self._fpath, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {self._fpath}")

        try:
            from user_strategy import UserStrategy
        except ImportError as e:
            raise StrategyLoadError(
                "Failed to import strategy. Class UserStrategy not found", e
            ) from e

        try:
            return UserStrategy(
                ohlc_feed_client=self._ohlc_feed_client,
                oms_client=self._oms_client,
                historical_data_client=self._historical_data_client,
            )
        except TypeError as e:
            raise StrategyLoadError("Failed to construct strategy", e) from e
