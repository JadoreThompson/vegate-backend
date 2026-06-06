from __future__ import annotations

from typing import TYPE_CHECKING, Generator

from vegate.markets.feed.client import OHLCFeedClient
from vegate.markets.schema import OHLC as OHLCSchema
from .ohlc_feed_client import BacktestOHLCFeedClient

if TYPE_CHECKING:
    from ..runner import BacktestRunner


class BacktestOHLCFeedClientProxy(OHLCFeedClient):

    def __init__(
        self, ohlc_feed_client: BacktestOHLCFeedClient, runner: BacktestRunner
    ):
        super().__init__()
        self._ohlc_feed_client = ohlc_feed_client
        self._runner = runner

    def candles(self) -> Generator[OHLCSchema, None, None]:
        for candle in self._ohlc_feed_client.candles():
            if not self._runner.is_running:
                raise Exception("Runner stopped, exiting candle generator")

            yield candle

    def __getattribute__(self, name):
        if name in ("_ohlc_feed_client", "_runner", "_logger", "candles", "__class__"):
            return super().__getattribute__(name)
        return self._ohlc_feed_client.__getattribute__(name)
