import logging
from datetime import UTC, datetime
from typing import AsyncIterator

import aiohttp
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame
from sqlalchemy import insert

from enums import BrokerType, Timeframe
from infra.db.models import OHLCs
from infra.db.utils import get_db_sess
from models import OHLC
from .base import BaseLoader


class AlpacaLoader(BaseLoader):
    """Loader for fetching historical candles from Alpaca API and persisting to database."""

    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca loader.

        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
            paper: Whether to use paper trading (default: True)
        """
        super().__init__(BrokerType.ALPACA)

        self.api_key = api_key
        self.secret_key = secret_key
        self._logger = logging.getLogger(self.__class__.__name__)

        self._http_sess: aiohttp.ClientSession | None = None
        self.client = StockHistoricalDataClient(api_key, secret_key)

    async def load_candles(
        self,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> AsyncIterator[OHLC]:
        """Asynchronously load historical candles from Alpaca API and persist to database.

        Note: Alpaca's historical data client is synchronous, so this
        implementation wraps the synchronous call.

        Args:
            symbol: Trading symbol (e.g., 'AAPL')
            timeframe: Candle timeframe (e.g., Timeframe.ONE_MINUTE)
            start_date: Start date for historical data
            end_date: End date for historical data

        Yields:
            OHLC candles in chronological order
        """
        symbol = symbol.upper()
        async for bar_batch in self._fetch_bars(
            symbol, timeframe, start_date, end_date
        ):
            # with open("f.json", "w") as f:
            #     import json

            #     json.dump(bar_batch, f)
            records = [
                {
                    "source": BrokerType.ALPACA,
                    "symbol": symbol,
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "timestamp": int(datetime.fromisoformat(bar["t"]).timestamp()),
                    "timeframe": timeframe,
                }
                for bar in bar_batch
            ]

            async with get_db_sess() as db_sess:
                await db_sess.execute(insert(OHLCs), records)
                await db_sess.commit()

            self._logger.info(f"Persisted {len(bar_batch)} bars for {symbol}")

    async def _fetch_bars(
        self,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ):
        if self._http_sess is None:
            self._http_sess = aiohttp.ClientSession(
                headers={
                    "APCA-API-KEY-ID": self.api_key,
                    "APCA-API-SECRET-KEY": self.secret_key,
                }
            )

        url = "https://data.alpaca.markets/v2/stocks/bars"

        fmt_start = datetime.strftime(start_date, "%Y-%m-%d")
        fmt_end = datetime.strftime(end_date, "%Y-%m-%d")
        self._logger.info(
            f"Fetching historical data for {symbol} from {fmt_start} to {fmt_end}"
        )

        params = {
            "symbols": symbol,
            "start": fmt_start,
            "end": fmt_end,
            "timeframe": self._timeframe_2_alpaca_timeframe(timeframe).value,
        }
        next_page_token = None
        page_count = 0

        while True:
            page_count += 1
            if next_page_token is not None:
                params["page_token"] = next_page_token

            rsp = await self._http_sess.get(url, params=params)
            data: dict = await rsp.json()
            if not rsp.ok:
                raise RuntimeError(f"Error in client response {data}")

            yield data["bars"][symbol.upper()]

            next_page_token = data["next_page_token"]
            if next_page_token is None:
                break

    @staticmethod
    def _timeframe_2_alpaca_timeframe(timeframe: Timeframe) -> AlpacaTimeFrame:
        """Map our Timeframe enum to Alpaca TimeFrame.

        Args:
            timeframe: Our Timeframe enum

        Returns:
            Alpaca TimeFrame

        Raises:
            ValueError: If timeframe is not supported
        """
        mapping = {
            Timeframe.m1: AlpacaTimeFrame.Minute,
            Timeframe.m5: AlpacaTimeFrame(5, "Min"),
            Timeframe.m15: AlpacaTimeFrame(15, "Min"),
            Timeframe.m30: AlpacaTimeFrame(30, "Min"),
            Timeframe.H1: AlpacaTimeFrame.Hour,
            Timeframe.H4: AlpacaTimeFrame(4, "Hour"),
            Timeframe.D1: AlpacaTimeFrame.Day,
        }

        if timeframe not in mapping:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        return mapping[timeframe]
