from datetime import datetime, UTC

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.schema import PaginatedResponse
from vegate.markets.enums import MarketType, Timeframe
from vegate.markets.schema import OHLC as OHLCResponse
from vegate.oms.enums import BrokerType
from .exception import SymbolNotFoundException
from .model import Instrument, InstrumentTimeframe, OHLC
from .schema import InstrumentInfo


class MarketsService:

    def __init__(self):
        pass

    async def get_symbols_info(
        self,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
        symbol: str | None = None,
        market_types: list[MarketType] | None = None,
        broker_types: list[BrokerType] | None = None,
        timeframes: list[Timeframe] | None = None,
    ) -> PaginatedResponse[InstrumentInfo]:
        stmt = (
            select(
                Instrument.id,
                Instrument.broker_type,
                Instrument.market_type,
                Instrument.native_symbol,
                InstrumentTimeframe.timeframe,
                InstrumentTimeframe.start_ts,
                InstrumentTimeframe.end_ts,
            )
            .join(InstrumentTimeframe, Instrument.id == InstrumentTimeframe.instrument_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        if symbol is not None:
            stmt = stmt.where(Instrument.native_symbol == symbol)
        if market_types is not None:
            stmt = stmt.where(Instrument.market_type.in_(market_types))
        if broker_types is not None:
            stmt = stmt.where(Instrument.broker_type.in_(broker_types))
        if timeframes is not None:
            stmt = stmt.where(InstrumentTimeframe.timeframe.in_(timeframes))

        result = await db_sess.execute(stmt)

        rows = result.all()
        has_next = len(rows) > limit
        rows = rows[:limit]

        data = [
            InstrumentInfo(
                id=row.id,
                symbol=row.native_symbol,
                broker_type=row.broker_type,
                market_type=row.market_type,
                timeframe=row.timeframe,
                start_date=datetime.fromtimestamp(row.start_ts, UTC),
                end_date=datetime.fromtimestamp(row.end_ts, UTC),
            )
            for row in rows
        ]

        return PaginatedResponse(
            size=len(data), has_next=has_next, data=data, page=page
        )

    async def get_symbol_info(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        db_sess: AsyncSession,
    ) -> InstrumentInfo:
        res = await db_sess.execute(
            select(
                Instrument.id,
                Instrument.broker_type,
                InstrumentTimeframe.timeframe,
                Instrument.market_type,
                InstrumentTimeframe.start_ts,
                InstrumentTimeframe.end_ts,
            )
            .join(
                InstrumentTimeframe,
                Instrument.id == InstrumentTimeframe.instrument_id,
            )
            .where(
                Instrument.symbol == symbol,
                Instrument.broker_type == broker_type,
                Instrument.market_type == market_type,
                InstrumentTimeframe.timeframe == timeframe,
            )
        )

        row = res.first()

        if row is None:
            raise SymbolNotFoundException(symbol)

        return InstrumentInfo(
            id=row.id,
            symbol=symbol,
            broker_type=row.broker_type,
            market_type=row.market_type,
            timeframe=row.timeframe,
            start_date=datetime.fromtimestamp(row.start_ts, UTC),
            end_date=datetime.fromtimestamp(row.end_ts, UTC),
        )

    async def get_ohlc_bars(
        self,
        start_date: datetime,
        end_date: datetime,
        db_sess: AsyncSession,
        *,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        page: int = 1,
        limit: int = 50,
        # start_time: int | None = None,
        # end_time: int | None = None,
    ) -> PaginatedResponse[OHLCResponse]:
        stmt = (
            select(
                OHLC.open,
                OHLC.high,
                OHLC.low,
                OHLC.close,
                OHLC.volume,
                OHLC.timestamp,
                OHLC.timeframe,
                Instrument.native_symbol,
                Instrument.broker_type,
                Instrument.market_type,
            )
            .join(Instrument, OHLC.instrument_id == Instrument.id)
            .where(
                Instrument.symbol == symbol,
                Instrument.market_type == market_type,
                Instrument.broker_type == broker_type,
                OHLC.timeframe == timeframe,
                OHLC.timestamp >= int(start_date.timestamp()),
                OHLC.timestamp <= int(end_date.timestamp()),
            )
            .order_by(OHLC.timestamp.asc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        result = await db_sess.execute(stmt)
        rows = result.all()
        has_next = len(rows) > limit
        rows = rows[:limit]

        data = [
            OHLCResponse(
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
                timestamp=row.timestamp,
                timeframe=row.timeframe,
                symbol=row.native_symbol,
                broker=row.broker_type,
                market_type=row.market_type,
            )
            for row in rows
        ]

        return PaginatedResponse(
            size=len(data), has_next=has_next, data=data, page=page
        )
