import asyncio
import logging

from aiohttp import ClientSession
from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
from db_models import MarketData
from utils.db import get_db_sess
from utils.utils import get_datetime
from .rate_limiter import RateLimiter


logger = logging.getLogger(__name__)


class AlpacaPipeline:
    def __init__(self):
        self._source = "alpaca"
        self._base_url = "https://data.alpaca.markets/v2"
        self._http_sess: ClientSession | None = None
        self._req_per_min = 200
        self._req_count = 0
        self._rate_limiter = RateLimiter(max_requests=200, per_seconds=60)
        self._http_sess_lock = asyncio.Lock()

    @property
    def source(self):
        return self._source

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
            await self._http_sess.close()

    async def __aenter__(self):
        await self.initialise()
        return self

    async def __aexit__(self, exc_type, exc_value, tcb):
        await self.cleanup()

    async def get_last_timestamp(self, symbol: str) -> datetime | None:
        """
        Query the database for the last timestamp of the given symbol from this source.
        Returns None if no data exists.
        """
        async with get_db_sess() as db_sess:
            stmt = (
                select(func.max(MarketData.timestamp))
                .where(MarketData.source == self._source)
                .where(MarketData.symbol == symbol)
            )
            last_timestamp = await db_sess.scalar(stmt)

            if last_timestamp is not None:
                # Convert Unix timestamp to datetime
                return datetime.fromtimestamp(last_timestamp)

    async def run_stocks_pipeline(self, symbol: str) -> None:
        """
        Run the stocks pipeline for the given symbol.
        Automatically resumes from the last timestamp in the database if data exists.
        """
        last_dt = await self.get_last_timestamp(symbol)

        end = get_datetime() - timedelta(days=1)

        if last_dt is not None:
            start = last_dt - timedelta(days=1)
            logger.info(
                f"Resuming pipeline for {symbol} from {start} (last timestamp: {last_dt})"
            )
        else:
            start = end - timedelta(weeks=5 * 52)
            logger.info(f"Starting new pipeline for {symbol} from {start}")

        fmt_start = datetime.strftime(start, "%Y-%m-%d")
        fmt_end = datetime.strftime(end, "%Y-%m-%d")

        params = {}
        next_page_token = None

        count = 0
        while True:
            count += 1

            if next_page_token is None:
                params["start"] = fmt_start
                params["end"] = fmt_end
            else:
                params["page_token"] = next_page_token

            await self._rate_limiter.acquire()
            rsp = await self._http_sess.get(
                f"{self._base_url}/stocks/{symbol}/trades", params=params
            )
            data = await rsp.json()
            rsp.raise_for_status()

            trades = data.get("trades", [])

            if not trades:
                break

            async with get_db_sess() as db_sess:
                await self._ingest_stock_trades(symbol, trades, db_sess)

            next_page_token = data["next_page_token"]

    @staticmethod
    def generate_key(trade: dict):
        timestamp = datetime.fromisoformat(trade["t"]).timestamp()
        return f"{timestamp}:{trade['p']}:{trade['s']}"

    async def _ingest_stock_trades(
        self, symbol: str, trades: list[dict], db_sess: AsyncSession
    ):
        if not trades:
            return

        records = []
        now = get_datetime()
        for trade in trades:
            trade_id = trade["i"]
            
            if trade_id is None:
                continue
            
            if "u" in trade:
                continue

            records.append(
                {
                    "source": self._source,
                    "symbol": symbol,
                    "price": Decimal(trade["p"]),
                    "size": trade["s"],
                    "timestamp": datetime.fromisoformat(trade["t"]).timestamp(),
                    "created_at": now,
                    "key": self.generate_key(trade),
                }
            )

        if not records:
            return

        await db_sess.execute(
            insert(MarketData)
            .values(records)
            .on_conflict_do_nothing(index_elements=["source", "key"])
        )
        await db_sess.commit()
        logger.info(f"Inserted {len(records)} trades successfully")
