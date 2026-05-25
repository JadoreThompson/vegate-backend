from datetime import datetime, UTC

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.schema import PaginatedResponse
from module.broker.enums import BrokerType
from .enums import MarketType, Timeframe
from .exception import SymbolNotFoundException
from .model import OHLC, Instrument
from .schema import InstrumentInfo
from .schema import OHLC as OHLCResponse


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
        broker_type: BrokerType | None = None,
        market_type: MarketType | None = None,
        timeframe: Timeframe | None = None,
    ) -> PaginatedResponse[InstrumentInfo]:
        stmt = (
            select(
                Instrument.id,
                Instrument.broker_type,
                Instrument.market_type,
                Instrument.symbol,
                OHLC.timeframe,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
            .group_by(Instrument.id, OHLC.timeframe)
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        if symbol is not None:
            stmt = stmt.where(Instrument.symbol == symbol)
        if broker_type is not None:
            stmt = stmt.where(Instrument.broker_type == broker_type)
        if market_type is not None:
            stmt = stmt.where(Instrument.market_type == market_type)
        if timeframe is not None:
            stmt = stmt.where(OHLC.timeframe == timeframe)

        result = await db_sess.execute(stmt)

        rows = result.all()
        has_next = len(rows) > limit
        rows = rows[:limit]

        data = [
            InstrumentInfo(
                id=row.id,
                symbol=row.symbol,
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
                OHLC.timeframe,
                Instrument.market_type,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            ).where(
                Instrument.symbol == symbol,
                Instrument.broker_type == broker_type,
                Instrument.market_type == market_type,
                OHLC.timeframe == timeframe,
            )
            .group_by(Instrument.id, OHLC.timeframe)
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
        db_sess: AsyncSession,
        *,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        page: int = 1,
        limit: int = 50,
        start_time: int | None = None,
        end_time: int | None = None,
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
                Instrument.symbol,
                Instrument.broker_type,
                Instrument.market_type,
            )
            .join(Instrument, OHLC.instrument_id == Instrument.id)
            .where(
                Instrument.symbol == symbol,
                Instrument.market_type == market_type,
                Instrument.broker_type == broker_type,
                OHLC.timeframe == timeframe,
            )
            .order_by(OHLC.timestamp.asc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        if start_time is not None:
            stmt = stmt.where(OHLC.timestamp >= start_time)
        if end_time is not None:
            stmt = stmt.where(OHLC.timestamp <= end_time)

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
                symbol=row.symbol,
                broker=row.broker_type,
                market_type=row.market_type,
            )
            for row in rows
        ]

        return PaginatedResponse(
            size=len(data), has_next=has_next, data=data, page=page
        )
