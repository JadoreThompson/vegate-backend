import asyncio
import logging
from collections import defaultdict

from vegate.markets.enums import MarketType, Timeframe
from vegate.oms.enums import BrokerType
from .base import OHLCFeed


class FeedManager:

    def __init__(self):
        self._symbol_market_broker_timeframes = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        self._feeds: list[OHLCFeed] = []
        self._feeds_lock: asyncio.Lock = asyncio.Lock()
        self._stopped = False
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_symbols(self):
        return set(self._symbol_market_broker_timeframes.keys())

    def get_market_types(self, symbol: str):
        return set(self._symbol_market_broker_timeframes.get(symbol, {}).keys())

    def get_brokers(self, symbol: str, market_type: MarketType):
        return set(
            self._symbol_market_broker_timeframes.get(symbol, {})
            .get(market_type, {})
            .keys()
        )

    def get_timeframes(
        self,
        symbol: str,
        market_type: MarketType,
        broker: BrokerType,
    ):
        return set(
            self._symbol_market_broker_timeframes.get(symbol, {})
            .get(market_type, {})
            .get(broker, [])
        )

    async def register(self, feed: OHLCFeed):
        async with self._feeds_lock:
            if self._stopped:
                raise ValueError("Manager has been stopped")

            self._feeds.append(feed)

            for sym in feed.symbols:
                for tf in feed.timeframes:
                    self._symbol_market_broker_timeframes[sym][feed.market_type][
                        feed.broker
                    ].append(tf)

            self._logger.info(f"Registered feed {feed.name}")

    async def stop_all(self):
        self._stopped = True
        excs = []

        async with self._feeds_lock:
            for feed in self._feeds:
                try:
                    await feed.stop()
                except Exception as e:
                    excs.append(e)

        if excs:
            raise ExceptionGroup(
                "Received exceptions whilst stopping feeds",
                excs,
            )
