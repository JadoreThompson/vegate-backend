import logging
from datetime import datetime
from typing import Any
from uuid import UUID

import aiohttp
from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame
from sqlalchemy import delete, func, insert, select

from core.db import get_db_session
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC as OHLCSchema
from vegate.oms.enums import BrokerType
from .base import OHLCLoader
from .schema import OHLCLoadResult
from ..aggregator import CandleAggregator
from ..model import Instrument, OHLC


class AlpacaOHLCLoader(OHLCLoader):
    """Loader for fetching historical candles from Alpaca API and persisting to database."""

    def __init__(self, api_key: str, secret_key: str):
        """Initialize the Alpaca loader.

        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
        """
        super().__init__()
        self._api_key = api_key
        self._secret_key = secret_key
        self._http_sess: aiohttp.ClientSession | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def close(self):
        if self._http_sess is not None and not self._http_sess.closed:
            await self._http_sess.close()

    async def load_candles(
        self,
        symbol: str,
        market_type: MarketType,
        timeframes: list[Timeframe],
        start_date: datetime,
        end_date: datetime,
    ) -> OHLCLoadResult:
        instrument_id = await self._get_or_create_instrument_id(symbol, market_type)

        if len(timeframes) == 1:
            return await self._load_single(
                symbol, market_type, timeframes[0],
                start_date, end_date, instrument_id,
            )

        return await self._load_multi(
            symbol, market_type, timeframes,
            start_date, end_date, instrument_id,
        )

    async def _load_single(
        self,
        symbol: str,
        market_type: MarketType,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        instrument_id: UUID,
    ) -> OHLCLoadResult:
        total_count = 0
        async for bars, _ in self._fetch_bars(
            market_type, symbol, timeframe, start_date, end_date
        ):
            records = self._build_records(bars, timeframe, instrument_id)
            count = await self._persist_records(instrument_id, symbol, timeframe, records)
            total_count += count

        return OHLCLoadResult(count=total_count, start_date=start_date, end_date=end_date)

    async def _load_multi(
        self,
        symbol: str,
        market_type: MarketType,
        timeframes: list[Timeframe],
        start_date: datetime,
        end_date: datetime,
        instrument_id: UUID,
    ) -> OHLCLoadResult:
        sorted_tfs = sorted(timeframes, key=lambda tf: tf.get_seconds())
        lowest_tf = sorted_tfs[0]
        higher_tfs = sorted_tfs[1:]

        aggregators = {tf: CandleAggregator(tf) for tf in higher_tfs}
        counts: dict[Timeframe, int] = {tf: 0 for tf in timeframes}
        per_tf_pending: dict[Timeframe, list[dict]] = {tf: [] for tf in timeframes}

        _BATCH = 200

        async for bars, _ in self._fetch_bars(
            market_type, symbol, lowest_tf, start_date, end_date
        ):
            for raw in bars:
                record = self._raw_to_record(raw, lowest_tf, instrument_id)
                per_tf_pending[lowest_tf].append(record)

                schema = self._raw_to_schema(raw, lowest_tf, symbol, market_type)
                for tf, agg in aggregators.items():
                    completed = agg.add_bar(schema)
                    if completed is not None:
                        per_tf_pending[tf].append(
                            self._schema_to_record(completed, instrument_id)
                        )

            for tf in timeframes:
                batch = per_tf_pending[tf]
                # if len(batch) >= _BATCH:
                #     counts[tf] += await self._persist_records(
                #         instrument_id, symbol, tf, batch
                #     )
                #     per_tf_pending[tf] = []
                counts[tf] += await self._persist_records(
                    instrument_id, symbol, tf, batch
                )
                per_tf_pending[tf] = []

        for tf in timeframes:
            if per_tf_pending[tf]:
                counts[tf] += await self._persist_records(
                    instrument_id, symbol, tf, per_tf_pending[tf]
                )

        total = sum(counts.values())
        return OHLCLoadResult(count=total, start_date=start_date, end_date=end_date)

    async def _get_or_create_instrument_id(
        self, symbol: str, market_type: MarketType
    ) -> UUID:
        """Fetch existing instrument or create a new one."""
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
                    symbol=self._format_symbol(symbol),
                    native_symbol=symbol,
                    market_type=market_type,
                    broker_type=BrokerType.ALPACA,
                )
                .returning(Instrument.id)
            )
            await db_sess.commit()
            return instrument_id

    async def _fetch_bars(
        self,
        market_type: MarketType,
        symbol: str | None = None,
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

        if params is None:
            fmt_start = datetime.strftime(start_date, "%Y-%m-%d")
            fmt_end = datetime.strftime(end_date, "%Y-%m-%d")

            params = {
                "symbols": symbol,
                "start": fmt_start,
                "end": fmt_end,
                "timeframe": self._timeframe_2_alpaca_timeframe(timeframe).value,
            }
        else:
            symbol = params["symbols"]

        url = self._get_url(market_type)
        next_page_token = None
        session = self._get_http_session()

        while True:
            if next_page_token is not None:
                params["page_token"] = next_page_token
            
            rsp = await session.get(url, params=params)
            data: dict = await rsp.json()

            if (
                rsp.status == 403
                and data.get("message")
                == "subscription does not permit querying recent SIP data"
            ):
                self._logger.warning(
                    f"Received 403 with message 'subscription does not permit querying recent SIP data'. "
                    f"Decrementing end date by 1 day to {params['end']} and retrying..."
                )
                break

            if not rsp.ok:
                raise RuntimeError(
                    f"Error in client response status: {rsp.status} - {data}"
                )

            yield data["bars"][symbol], params

            next_page_token = data["next_page_token"]
            if next_page_token is None:
                break

    def _get_http_session(self):
        if self._http_sess is None:
            self._http_sess = aiohttp.ClientSession(
                headers={
                    "APCA-API-KEY-ID": self._api_key,
                    "APCA-API-SECRET-KEY": self._secret_key,
                }
            )
        return self._http_sess

    def _get_url(self, market_type: MarketType):
        if market_type == MarketType.STOCKS:
            return "https://data.alpaca.markets/v2/stocks/bars"
        if market_type == MarketType.CRYPTO:
            return "https://data.alpaca.markets/v1beta3/crypto/us/bars"
        raise ValueError(f"Unsupported market type: {market_type}")

    async def _persist_records(
        self,
        instrument_id: UUID,
        symbol: str,
        timeframe: Timeframe,
        records: list[dict[str, Any]],
    ) -> int:
        """Persist records to DB with an idempotency check.

        Returns:
            Number of records actually inserted (0 if the batch already exists).
        """
        if not records:
            return 0

        sdate = records[0]["timestamp"]
        edate = records[-1]["timestamp"]
        fsdate = datetime.fromtimestamp(sdate)
        fedate = datetime.fromtimestamp(edate)

        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(func.count(OHLC.id)).where(
                    OHLC.instrument_id == instrument_id,
                    OHLC.timestamp.between(sdate, edate),
                    OHLC.timeframe == timeframe
                )
            )
            row = res.first()
            existing_count = row[0] if row is not None else 0

            self._logger.info(
                "Found %s existing OHLCs for %s from %s to %s timeframe %s. Fetched %s",
                existing_count,
                symbol,
                fsdate,
                fedate,
                timeframe,
                len(records)
            )

            if existing_count == len(records):
                self._logger.info("Count matches, skipping deletion and insertion")
                return 0

            if existing_count:
                self._logger.info(
                    "Existing OHLCs found for %s from %s to %s, deleting...",
                    symbol,
                    fsdate,
                    fedate,
                )
                await db_sess.execute(
                    delete(OHLC).where(
                        OHLC.instrument_id == instrument_id,
                        OHLC.timeframe == timeframe,
                        OHLC.timestamp.between(sdate, edate),
                    )
                )

            await db_sess.execute(insert(OHLC), records)
            await db_sess.commit()
            return len(records)

    def _raw_to_record(
        self, candle: dict[str, Any], timeframe: Timeframe, instrument_id: UUID
    ) -> dict:
        dt = datetime.fromisoformat(candle["t"])
        parsed_candle = {
            "open": candle["o"],
            "high": candle["h"],
            "low": candle["l"],
            "close": candle["c"],
            "timestamp": int(dt.timestamp()),
            "timeframe": timeframe,
            "volume": candle["v"],
            "instrument_id": str(instrument_id),
        }
        return parsed_candle

    def _build_records(
        self, candles: list[dict[str, Any]], timeframe: Timeframe, instrument_id: UUID
    ) -> list[dict]:
        return [
            self._raw_to_record(candle, timeframe, instrument_id) for candle in candles
        ]

    def _raw_to_schema(
        self,
        raw: dict[str, Any],
        timeframe: Timeframe,
        symbol: str,
        market_type: MarketType,
    ) -> OHLCSchema:
        dt = datetime.fromisoformat(raw["t"])
        return OHLCSchema(
            open=raw["o"],
            high=raw["h"],
            low=raw["l"],
            close=raw["c"],
            volume=raw["v"],
            timestamp=int(dt.timestamp()),
            timeframe=timeframe,
            symbol=symbol,
            broker=BrokerType.ALPACA,
            market_type=market_type,
        )

    def _schema_to_record(
        self, schema: OHLCSchema, instrument_id: UUID
    ) -> dict:
        return {
            "open": schema.open,
            "high": schema.high,
            "low": schema.low,
            "close": schema.close,
            "volume": schema.volume,
            "timestamp": schema.timestamp,
            "timeframe": schema.timeframe,
            "instrument_id": str(instrument_id),
        }

    def _format_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "")

    def _timeframe_2_alpaca_timeframe(self, timeframe: Timeframe) -> AlpacaTimeFrame:
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
