from datetime import datetime, UTC

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.markets.exception import SymbolNotFoundException
from api.routes.markets.model import InstrumentInfo
from enums import BrokerType, MarketType, Timeframe
from infra.db.model import OHLC
from infra.db.model.instrument import Instrument


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
            # select(
            #     OHLC.source,
            #     OHLC.timeframe,
            #     OHLC.market_type,
            #     OHLC.symbol,
            #     func.min(OHLC.timestamp).label("start_ts"),
            #     func.max(OHLC.timestamp).label("end_ts"),
            # )
            # .group_by(OHLC.symbol, OHLC.source, OHLC.market_type, OHLC.timeframe)
            select(
                Instrument.id,
                Instrument.broker_type,
                Instrument.market_type,
                Instrument.symbol,
                OHLC.timeframe,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
            .group_by(
                # Instrument.symbol,
                # Instrument.broker_type,
                # Instrument.market_type,
                Instrument.id,
                OHLC.timeframe,
            )
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
            )
            .where(
                Instrument.symbol == symbol,
                Instrument.broker_type == broker_type,
                Instrument.market_type == market_type,
                OHLC.timeframe == timeframe,
            )
            # .group_by(Instrument.broker_type, Instrument.market_type, OHLC.timeframe)
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
