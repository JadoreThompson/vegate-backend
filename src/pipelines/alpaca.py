import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Type

from aiohttp import ClientSession
from alpaca.data.live import StockDataStream, CryptoDataStream
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, REDIS_BROKER_TRADE_EVENTS_KEY
from core.events import BrokerTradeEvent
from db_models import Ticks
from engine.enums import BrokerType, MarketType
from utils.db import get_db_sess
from utils.redis import REDIS_CLIENT
from utils.utils import get_datetime
from .base import BasePipeline
from .rate_limiter import RateLimiter


class AlpacaPipeline(BasePipeline):
    """
    Pipeline for ingesting market data from Alpaca.

    Handles both historical data fetching and live streaming for stocks and crypto.
    Methods are organized in logical groups for better maintainability.
    """

    def __init__(self):
        self._source = "alpaca"
        self._base_url = "https://data.alpaca.markets"
        self._http_sess: ClientSession | None = None
        self._rate_limiter = RateLimiter(max_requests=200, per_seconds=60)
        self._logger = logging.getLogger(type(self).__name__)

    async def initialise(self):
        if self._http_sess is None:
            self._http_sess = ClientSession(
                headers={
                    "APCA-API-KEY-ID": ALPACA_API_KEY,
                    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
                }
            )

    async def cleanup(self):
        if self._http_sess is not None and not self._http_sess.closed:
            self._logger.debug("Closing Alpaca pipeline HTTP session")
            await self._http_sess.close()
            self._logger.debug("Alpaca pipeline cleaned up successfully")

    async def __aenter__(self):
        await self.initialise()
        return self

    async def __aexit__(self, exc_type, exc_value, tcb):
        await self.cleanup()

    async def run_crypto_pipeline(self, symbol: str):
        """Run the complete crypto data pipeline: historical + live streaming."""
        self._logger.info(f"Starting crypto pipeline for {symbol}")
        last_dt = await self._get_last_timestamp(symbol, MarketType.CRYPTO)

        end_date = get_datetime() - timedelta(days=1)
        start_date = (
            last_dt - timedelta(days=1)
            if last_dt
            else end_date - timedelta(weeks=5 * 52)
        )

        if last_dt:
            self._logger.info(f"Resuming from last timestamp: {last_dt}")
        else:
            self._logger.info(f"Starting fresh from {start_date}")

        await self._fetch_historical(symbol, MarketType.CRYPTO, start_date, end_date)

        task = None

        try:
            task = asyncio.create_task(
                self._loop_historical(symbol, MarketType.CRYPTO, start_date, end_date)
            )
            while True:
                await self._stream_trades(symbol, MarketType.CRYPTO)
        finally:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def run_stocks_pipeline(self, symbol: str) -> None:
        """Run the complete stocks data pipeline: historical + live streaming."""
        raise NotImplementedError()

    async def _fetch_historical(
        self,
        symbol: str,
        market_type: MarketType,
        start_date: datetime,
        end_date: datetime,
    ):
        """Fetch historical trade data from Alpaca API."""
        if market_type not in {MarketType.CRYPTO, MarketType.STOCKS}:
            raise NotImplementedError(
                f"Fetch historical implementation for market type '{market_type}' not implemented"
            )

        fmt_start = datetime.strftime(start_date, "%Y-%m-%d")
        fmt_end = datetime.strftime(end_date, "%Y-%m-%d")
        self._logger.info(
            f"Fetching historical {market_type.value} data for {symbol} from {fmt_start} to {fmt_end}"
        )
        params = {}
        next_page_token = None
        page_count = 0

        while True:
            page_count += 1
            params = (
                {"start": fmt_start, "end": fmt_end}
                if not next_page_token
                else {"page_token": next_page_token}
            )

            if market_type == MarketType.CRYPTO:
                endpoint = f"{self._base_url}/v1beta3/crypto/us/trades"
                params["symbols"] = [symbol]
            elif market_type == MarketType.STOCKS:
                endpoint = f"{self._base_url}/v2/stocks/{symbol}/trades"

            self._logger.debug(
                f"Fetching page {page_count} for {symbol} ({market_type.value})"
            )
            await self._rate_limiter.acquire()
            rsp = await self._http_sess.get(endpoint, params=params)
            rsp.raise_for_status()
            data: dict = await rsp.json()

            if market_type == MarketType.STOCKS:
                trades = data.get("trades", [])
            elif market_type == MarketType.CRYPTO:
                trades = data.get("trades", {}).get(symbol)

            if not trades:
                self._logger.info(
                    f"No more trades available for {symbol} ({market_type.value})"
                )
                break

            self._logger.debug(f"Retrieved {len(trades)} trades on page {page_count}")
            async with get_db_sess() as db_sess:
                await self._ingest_historical_trades(
                    trades, symbol, market_type, db_sess
                )

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                self._logger.info(
                    f"Completed fetching historical data for {symbol} ({market_type.value}) - {page_count} pages processed"
                )
                break

    async def _loop_historical(
        self,
        symbol: str,
        market_type: MarketType,
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        """Continuously refresh historical data on a daily schedule."""
        self._logger.info(
            f"Starting daily historical refresh loop for {symbol} ({market_type.value})"
        )
        while True:
            await asyncio.sleep(86400)  # 1 day
            self._logger.info(
                f"Running daily historical refresh for {symbol} ({market_type.value})"
            )
            end_date = get_datetime() - timedelta(days=1)
            start_date = end_date - timedelta(days=1)
            await self._fetch_historical(symbol, market_type, start_date, end_date)
            self._logger.info(
                f"Completed daily historical refresh for {symbol} ({market_type.value})"
            )

    async def _ingest_historical_trades(
        self,
        trades: list[dict],
        symbol: str,
        market_type: MarketType,
        db_sess: AsyncSession,
    ):
        """Process and store historical trades in the database."""
        if not trades:
            self._logger.debug(f"No trades to ingest for {symbol}")
            return

        self._logger.debug(
            f"Processing {len(trades)} trades for {symbol} ({market_type.value})"
        )
        now = get_datetime()
        records = [
            {
                "source": self._source,
                "symbol": symbol,
                "market_type": market_type,
                "price": trade["p"],
                "size": trade["s"],
                "timestamp": datetime.fromisoformat(trade["t"]).timestamp(),
                "created_at": now,
                "key": self._generate_trade_key(trade),
            }
            for trade in trades
            if "u" not in trade  # skip update trades if present
        ]

        if not records:
            self._logger.debug(f"All trades filtered out for {symbol} (update trades)")
            return

        await db_sess.execute(
            insert(Ticks)
            .values(records)
            .on_conflict_do_nothing(index_elements=["source", "key"])
        )
        await db_sess.commit()
        self._logger.info(
            f"Inserted {len(records)} {market_type.value} trades for {symbol}"
        )

    async def _stream_trades(self, symbol: str, market_type: MarketType):
        """Stream live trades from Redis pub/sub and ingest to database."""

        batch = []
        batch_size = 1000

        async with REDIS_CLIENT.pubsub() as ps:
            await ps.subscribe(REDIS_BROKER_TRADE_EVENTS_KEY)
            async for msg in ps.listen():
                if msg["type"] == "connect":
                    continue

                try:
                    event = BrokerTradeEvent(**json.loads(msg["data"]))
                    if event.broker != BrokerType.ALPACA or event.symbol != symbol:
                        continue

                    batch.append(event)
                    if len(batch) == batch_size:
                        async with get_db_sess() as db_sess:
                            await self._ingest_live_trades(batch, market_type, db_sess)
                        batch = []

                except (json.JSONDecodeError, ValidationError):
                    pass

    async def _ingest_live_trades(
        self, trades: list[dict], market_type: MarketType, db_sess: AsyncSession
    ):
        """Process and store live trades in the database."""
        records = [self._parse_live_trade(trade, market_type) for trade in trades]

        await db_sess.execute(
            insert(Ticks)
            .values(records)
            .on_conflict_do_nothing(index_elements=["source", "key"])
        )

        await db_sess.commit()

    @staticmethod
    def _generate_trade_key(trade: BrokerTradeEvent) -> str:
        """Generate unique key for trade deduplication."""
        return f"{trade['t']}:{trade['p']}:{trade['s']}"

    def _parse_live_trade(
        self, trade: BrokerTradeEvent, market_type: MarketType
    ) -> dict:
        """Parse live trade event into database record format."""
        return {
            "source": self._source,
            "symbol": trade.symbol,
            "market_type": market_type,
            "price": trade.price,
            "size": trade.quantity,
            "timestamp": trade.timestamp,
            "created_at": get_datetime(),
            "key": self._generate_trade_key(trade),
        }

    async def _get_last_timestamp(
        self, symbol: str, market_type: MarketType
    ) -> datetime | None:
        """Query the most recent timestamp for a symbol from the database."""
        self._logger.debug(
            f"Querying last timestamp for {symbol} ({market_type.value})"
        )
        async with get_db_sess() as db_sess:
            last_timestamp = await db_sess.scalar(
                select(func.max(Ticks.timestamp))
                .where(Ticks.source == self._source)
                .where(Ticks.symbol == symbol)
                .where(Ticks.market_type == market_type)
            )
            if last_timestamp:
                dt = datetime.fromtimestamp(last_timestamp)
                self._logger.debug(
                    f"Found last timestamp for {symbol} ({market_type.value}): {dt}"
                )
                return dt
            else:
                self._logger.debug(
                    f"No existing data found for {symbol} ({market_type.value})"
                )
                return None
