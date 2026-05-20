import asyncio
import logging
from datetime import datetime, timedelta
from uuid import UUID

import aiohttp
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from enums import BrokerType, MarketType, Timeframe
from infra.db.model import OHLC
from infra.db.model.instrument import Instrument
from infra.db.utils import get_db_session
from service.ohlc.loader.loader_config import OHLCLoaderConfig
from service.ohlc.loader.logging.record import (
    OHLCLoadCompleteRecord,
    OHLCLoadStartRecord,
    OHLCLogRecord,
)
from service.ohlc.loader.logging.wal import WALogger
from service.ohlc.loader.models import LoadResult
from utils import get_datetime
from .base import BaseOHLCLoader


class AlpacaOHLCLoader(BaseOHLCLoader):
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
        self._walogger: WALogger | None = None
        self._http_sess: aiohttp.ClientSession | None = None
        self.client = StockHistoricalDataClient(api_key, secret_key)
        self._last_record: OHLCLogRecord | None = None
        self._instrument_id: UUID | None = None

    @property
    def instrument_id(self):
        return self._instrument_id

    def _get_walogger(self, symbol: str, timeframe: Timeframe) -> WALogger:
        if self._walogger is None:
            self._walogger = WALogger(
                BrokerType.ALPACA, self._format_symbol(symbol), timeframe
            )
        return self._walogger

    async def run(self, config: OHLCLoaderConfig) -> None:
        async def load_candles():
            await self.load_candles(
                config.symbol,
                config.market_type,
                config.timeframe,
                config.start_date,
                config.end_date,
            )

        self._instrument_id = await self._get_instrument_id(config)

        await load_candles()

        while True:
            self._logger.info(
                f"Sleeping for {config.poll_interval} seconds before next poll..."
            )
            await asyncio.sleep(config.poll_interval)
            await load_candles()

    async def _get_instrument_id(self, symbol: str, market_type: MarketType) -> UUID:
        # Fetches or creates instrument, returning it's id.
        async with get_db_session() as db_sess:
            instrument_id = await db_sess.scalar(
                select(Instrument.id).where(
                    Instrument.native_symbol == symbol,
                    Instrument.market_type == market_type,
                    Instrument.broker_type == BrokerType.ALPACA,
                )
            )

            if instrument_id is not None:
                return instrument_id

            instrument_id = await db_sess.scalar(
                insert(Instrument)
                .values(
                    symbol=symbol.replace("/", ""),
                    native_symbol=symbol,
                    market_type=market_type,
                    broker_type=BrokerType.ALPACA,
                )
                .returning(Instrument.id)
            )
            await db_sess.commit()
        return instrument_id

    async def load_candles(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> LoadResult:
        if self._instrument_id is None:
            self._instrument_id = await self._get_instrument_id(symbol, market_type)
        symbol = symbol.upper()
        fmt_symbol = self._format_symbol(symbol)
        walogger = self._get_walogger(symbol, timeframe)

        last_params = None
        if self._last_record is None:
            # self._last_record = self._restore(symbol, timeframe)
            last_params = await self._restore(
                symbol, market_type, timeframe, start_date, end_date
            )

        total_bars = 0
        first_date = None
        last_date = None

        async for bar_batch, fetch_params in self._fetch_bars(
            symbol,
            market_type,
            timeframe,
            start_date,
            end_date,
            # self._last_record.params if self._last_record is not None else None,
            last_params,
        ):
            self._last_record = OHLCLoadStartRecord(params=fetch_params)
            # walogger.log(self._last_record)
            await self._checkpoint(
                symbol, market_type, timeframe, start_date, end_date, fetch_params
            )

            records = []
            for bar in bar_batch:
                d = datetime.fromisoformat(bar["t"])
                if first_date is None:
                    datetime.fromisoformat(bar["t"])

                last_date = d

                records.append(
                    {
                        "open": bar["o"],
                        "high": bar["h"],
                        "low": bar["l"],
                        "close": bar["c"],
                        "timestamp": int(d.timestamp()),
                        "timeframe": timeframe,
                        "volume": bar["v"],
                        "instrument_id": self._instrument_id
                    }
                )

            async with get_db_session() as db_sess:
                sdate = records[0]["timestamp"]
                edate = records[-1]["timestamp"]
                fsdate = datetime.fromtimestamp(sdate)
                fedate = datetime.fromtimestamp(edate)

                res = await db_sess.execute(
                    select(func.count(OHLC.ohlc_id)).where(
                        OHLC.instrument_id == self._instrument_id,
                        OHLC.timestamp.between(sdate, edate),
                    )
                )
                data = res.first()
                if data is not None:
                    count = data[0]
                    self._logger.info(
                        f"Found {count} existing OHLCs for {symbol} from {fsdate} to {fedate}"
                    )
                    if count == len(records):
                        self._logger.info(
                            "Count matches, skipping deletion and insertion"
                        )
                        self._last_record = OHLCLoadCompleteRecord(
                            params=fetch_params, count=count
                        )
                        # walogger.log(self._last_record)
                        continue

                    self._logger.info(
                        "Count mismatch, deleting existing records and inserting new ones"
                    )

                    self._logger.info(
                        f"Existing OHLCs found for {symbol} from {fsdate} to {fedate}, deleting..."
                    )
                    await db_sess.execute(
                        delete(OHLC).where(
                            # OHLC.source == BrokerType.ALPACA,
                            # OHLC.symbol == fmt_symbol,
                            # OHLC.market_type == market_type,
                            OHLC.instrument_id == self._instrument_id,
                            OHLC.timeframe == timeframe,
                            OHLC.timestamp.between(sdate, edate),
                        )
                    )

                await db_sess.execute(insert(OHLC), records)
                await db_sess.commit()

            self._last_record = OHLCLoadCompleteRecord(params=fetch_params, count=count)
            # walogger.log(self._last_record)

            total_bars += len(records)
            self._logger.info(f"Persisted {len(bar_batch)} bars for {symbol}")

        self._logger.info(
            f"Finished loading candles for {symbol}. Total bars: {total_bars}"
        )

        return LoadResult(start_date=first_date, end_date=last_date, count=total_bars)

    async def _fetch_bars(
        self,
        symbol: str | None = None,
        market_type: MarketType | None = None,
        timeframe: Timeframe | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        params: dict | None = None,
    ):
        if params is None and (
            symbol is None
            or market_type is None
            or timeframe is None
            or start_date is None
            or end_date is None
        ):
            raise ValueError(
                "Either params or all of symbol, market_type, timeframe, start_date, end_date must be provided"
            )

        if self._http_sess is None:
            self._http_sess = aiohttp.ClientSession(
                headers={
                    "APCA-API-KEY-ID": self.api_key,
                    "APCA-API-SECRET-KEY": self.secret_key,
                }
            )

        url = None
        if market_type == MarketType.STOCKS:
            url = "https://data.alpaca.markets/v2/stocks/bars"
        elif market_type == MarketType.CRYPTO:
            url = "https://data.alpaca.markets/v1beta3/crypto/us/bars"
        else:
            raise ValueError(f"Unsupported market type: {market_type}")

        if params is None:
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

        while True:
            if next_page_token is not None:
                params["page_token"] = next_page_token

            rsp = await self._http_sess.get(url, params=params)
            data: dict = await rsp.json()
            if (
                rsp.status == 403
                and data.get("message")
                == "subscription does not permit querying recent SIP data"
            ):
                params["end"] = (
                    datetime.strptime(params["end"], "%Y-%m-%d") - timedelta(days=1)
                ).strftime("%Y-%m-%d")
                self._logger.warning(
                    f"Received 403 with message 'subscription does not permit querying recent SIP data'. "
                    f"Decrementing end date by 1 day to {params['end']} and retrying..."
                )
                continue

            if not rsp.ok:
                raise RuntimeError(
                    f"Error in client response status: {rsp.status} - {data}"
                )

            yield data["bars"][symbol.upper()], params

            next_page_token = data["next_page_token"]
            if next_page_token is None:
                break

    async def _restore(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> OHLCLogRecord | None: ...

    async def _checkpoint(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        params: dict,
    ) -> None: ...

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

    @staticmethod
    def _format_symbol(symbol: str) -> str:
        """Format symbol for consistent storage and comparison.

        Args:
            symbol: Original symbol (e.g., 'BTC/USD')
        Returns:
            Formatted symbol (e.g., 'BTCUSD')
        """
        return symbol.replace("/", "")
