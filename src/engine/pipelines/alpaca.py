import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Type

from aiohttp import ClientSession
from alpaca.data.live import StockDataStream, CryptoDataStream
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from db_models import MarketData
from engine.enums import MarketType
from utils.db import get_db_sess
from utils.utils import get_datetime
from .rate_limiter import RateLimiter


logger = logging.getLogger(__name__)


class AlpacaPipeline:
    def __init__(self):
        self._source = "alpaca"
        self._base_url = "https://data.alpaca.markets"
        self._http_sess: ClientSession | None = None
        self._rate_limiter = RateLimiter(max_requests=200, per_seconds=60)

    @property
    def source(self):
        return self._source

    async def initialise(self):
        if self._http_sess is None:
            logger.info("Initializing Alpaca pipeline HTTP session")
            self._http_sess = ClientSession(
                headers={
                    "APCA-API-KEY-ID": ALPACA_API_KEY,
                    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
                }
            )
            logger.info("Alpaca pipeline initialized successfully")

    async def cleanup(self):
        if self._http_sess and not self._http_sess.closed:
            logger.info("Closing Alpaca pipeline HTTP session")
            await self._http_sess.close()
            logger.info("Alpaca pipeline cleaned up successfully")

    async def __aenter__(self):
        await self.initialise()
        return self

    async def __aexit__(self, exc_type, exc_value, tcb):
        await self.cleanup()

    @staticmethod
    def _generate_trade_key(trade: dict) -> str:
        timestamp = datetime.fromisoformat(trade["t"]).timestamp()
        return f"{timestamp}:{trade['p']}:{trade['s']}"

    async def _get_last_timestamp(
        self, symbol: str, market_type: MarketType
    ) -> datetime | None:
        logger.debug(f"Querying last timestamp for {symbol} ({market_type.value})")
        async with get_db_sess() as db_sess:
            last_timestamp = await db_sess.scalar(
                select(func.max(MarketData.timestamp))
                .where(MarketData.source == self._source)
                .where(MarketData.symbol == symbol)
                .where(MarketData.market_type == market_type)
            )
            if last_timestamp:
                dt = datetime.fromtimestamp(last_timestamp)
                logger.info(
                    f"Found last timestamp for {symbol} ({market_type.value}): {dt}"
                )
                return dt
            else:
                logger.info(
                    f"No existing data found for {symbol} ({market_type.value})"
                )
                return None

    async def _ingest_trades(
        self,
        trades: list[dict],
        symbol: str,
        market_type: MarketType,
        db_sess: AsyncSession,
    ):
        if not trades:
            logger.debug(f"No trades to ingest for {symbol}")
            return

        logger.debug(
            f"Processing {len(trades)} trades for {symbol} ({market_type.value})"
        )
        now = get_datetime()
        records = [
            {
                "source": self._source,
                "symbol": symbol,
                "market_type": market_type,
                "price": Decimal(str(trade["p"])),
                "size": trade.get("s", 1),
                "timestamp": datetime.fromisoformat(trade["t"]).timestamp(),
                "created_at": now,
                "key": self._generate_trade_key(trade),
            }
            for trade in trades
            if "u" not in trade  # skip update trades if present
        ]

        if not records:
            logger.debug(f"All trades filtered out for {symbol} (update trades)")
            return

        await db_sess.execute(
            insert(MarketData)
            .values(records)
            .on_conflict_do_nothing(index_elements=["source", "key"])
        )
        await db_sess.commit()
        logger.info(
            f"Inserted {len(records)} {market_type.value} trades for {symbol}"
        )

    async def _fetch_historical(
        self,
        symbol: str,
        market_type: MarketType,
        start_date: datetime,
        end_date: datetime,
    ):
        if market_type not in {MarketType.CRYPTO, MarketType.STOCKS}:
            raise NotImplementedError(
                f"Fetch historical implementation for market type '{market_type}' not implemented"
            )

        fmt_start = datetime.strftime(start_date, "%Y-%m-%d")
        fmt_end = datetime.strftime(end_date, "%Y-%m-%d")
        logger.info(
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

            logger.debug(
                f"Fetching page {page_count} for {symbol} ({market_type.value})"
            )
            await self._rate_limiter.acquire()
            rsp = await self._http_sess.get(endpoint, params=params)
            rsp.raise_for_status()
            data = await rsp.json()

            if market_type == MarketType.STOCKS:
                trades = data.get("trades", [])
            elif market_type == MarketType.CRYPTO:
                trades = data.get("trades", {}).get(symbol)

            if not trades:
                logger.info(
                    f"No more trades available for {symbol} ({market_type.value})"
                )
                break

            logger.debug(f"Retrieved {len(trades)} trades on page {page_count}")
            async with get_db_sess() as db_sess:
                await self._ingest_trades(trades, symbol, market_type, db_sess)

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                logger.info(
                    f"Completed fetching historical data for {symbol} ({market_type.value}) - {page_count} pages processed"
                )
                break

    async def _stream_trades(
        self,
        symbol: str,
        stream_class: Type[StockDataStream] | Type[CryptoDataStream],
        market_type: MarketType,
    ):
        """Generic streaming handler for live trades."""
        logger.info(f"Starting live {market_type.value} stream for {symbol}")
        stream = stream_class(ALPACA_API_KEY, ALPACA_SECRET_KEY, raw_data=True)

        async def handle_trade(trade):
            logger.debug(
                f"Received live trade for {symbol}: price={trade.get('p')}, size={trade.get('s')}"
            )
            async with get_db_sess() as db_sess:
                await self._ingest_trades([trade], symbol, market_type, db_sess)

        stream.subscribe_trades(handle_trade, symbol)
        logger.info(f"Live stream connected for {symbol} ({market_type.value})")
        await stream._run_forever()

    async def run_stocks_pipeline(self, symbol: str) -> None:
        logger.info(f"Starting stocks pipeline for {symbol}")
        last_dt = await self._get_last_timestamp(symbol, MarketType.STOCKS)

        end_date = get_datetime() - timedelta(days=1)
        start_date = (
            last_dt - timedelta(days=1)
            if last_dt
            else end_date - timedelta(weeks=5 * 52)
        )

        if last_dt:
            logger.info(f"Resuming from last timestamp: {last_dt}")
        else:
            logger.info(f"Starting fresh from {start_date}")

        await self._fetch_historical(symbol, MarketType.STOCKS, start_date, end_date)

        task = asyncio.create_task(
            self._loop_historical(symbol, MarketType.STOCKS, start_date, end_date)
        )

        try:
            while True:
                await self._stream_trades(symbol, CryptoDataStream, MarketType.STOCKS)
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def run_crypto_pipeline(self, symbol: str):
        logger.info(f"Starting crypto pipeline for {symbol}")
        last_dt = await self._get_last_timestamp(symbol, MarketType.CRYPTO)

        end_date = get_datetime() - timedelta(days=1)
        start_date = (
            last_dt - timedelta(days=1)
            if last_dt
            else end_date - timedelta(weeks=5 * 52)
        )

        if last_dt:
            logger.info(f"Resuming from last timestamp: {last_dt}")
        else:
            logger.info(f"Starting fresh from {start_date}")

        await self._fetch_historical(symbol, MarketType.CRYPTO, start_date, end_date)

        task = None

        try:
            task = asyncio.create_task(
                self._loop_historical(symbol, MarketType.CRYPTO, start_date, end_date)
            )
            while True:
                await self._stream_trades(symbol, CryptoDataStream, MarketType.CRYPTO)
        finally:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _loop_historical(
        self,
        symbol: str,
        market_type: MarketType,
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        logger.info(
            f"Starting daily historical refresh loop for {symbol} ({market_type.value})"
        )
        while True:
            await asyncio.sleep(86400)  # 1 day
            logger.info(
                f"Running daily historical refresh for {symbol} ({market_type.value})"
            )
            end_date = get_datetime() - timedelta(days=1)
            start_date = end_date - timedelta(days=1)
            await self._fetch_historical(symbol, market_type, start_date, end_date)
            logger.info(
                f"Completed daily historical refresh for {symbol} ({market_type.value})"
            )
