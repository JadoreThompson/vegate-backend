import asyncio
import logging
from datetime import datetime, timedelta

from util import get_datetime
from vegate.oms.enums import BrokerType

from .schema import PollSubscription
from ..base import OHLCLoader
from ..schema import OHLCLoadResult


class OHLCPoller:
    """Periodically loads candles for a collection of subscriptions."""

    def __init__(
        self,
        loaders: dict[BrokerType, OHLCLoader],
        subscriptions: list[PollSubscription],
        poll_interval: float,
    ):
        self._loaders = loaders
        self._subscriptions = subscriptions
        self._poll_interval = poll_interval

        self._results: list[OHLCLoadResult | None] = [None] * len(subscriptions)

        self._running = False

        self._logger = logging.getLogger(self.__class__.__name__)

    async def run(self) -> None:
        """Run the poller until stopped."""
        self._running = True

        self._logger.info(
            "Starting OHLC poller with %s subscriptions",
            len(self._subscriptions),
        )

        try:
            while self._running:
                await self._poll_once()
                await asyncio.sleep(self._poll_interval)
        finally:
            self._running = False
            self._logger.info("OHLC poller stopped")

    async def _poll_once(self) -> None:
        for idx, subscription in enumerate(self._subscriptions):
            loader = self._loaders[subscription.broker]
            previous_result = self._results[idx]

            start_date = subscription.start_date
            end_date = subscription.end_date or self._get_current_end_date()

            if previous_result is not None:
                start_date = previous_result.end_date - timedelta(days=1)
                end_date = self._get_current_end_date()

            self._logger.info(
                "Loading candles: broker=%s symbol=%s timeframes=%s start=%s end=%s",
                subscription.broker,
                subscription.symbol,
                subscription.timeframes,
                start_date,
                end_date,
            )

            result = await loader.load_candles(
                symbol=subscription.symbol,
                market_type=subscription.market_type,
                timeframes=subscription.timeframes,
                start_date=start_date,
                end_date=end_date,
            )

            self._results[idx] = result

            self._logger.info(
                "Loaded %s candles for %s %s",
                result.count,
                subscription.symbol,
                subscription.timeframes,
            )

    @staticmethod
    def _get_current_end_date() -> datetime:
        return get_datetime() + timedelta(days=1)
