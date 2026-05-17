from datetime import datetime, UTC

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.markets.exception import SymbolNotFoundException
from api.routes.markets.model import OHLCInfo
from enums import BrokerType, MarketType, Timeframe
from infra.db.model import OHLC


class MarketsService:

    def __init__(self):
        pass

    async def get_symbols_info(self, db_sess: AsyncSession, page: int, limit: int, symbol: str | None = None,
                               broker_type: BrokerType | None = None, market_type: MarketType | None = None,
                               timeframe: Timeframe | None = None) -> PaginatedResponse[OHLCInfo]:
        stmt = \
            (select(
                OHLC.source,
                OHLC.timeframe,
                OHLC.market_type,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
             .where(OHLC.symbol == symbol)
             .group_by(OHLC.source, OHLC.market_type, OHLC.timeframe)
             .offset((page - 1) * limit)
             .limit(limit + 1))

        if broker_type is not None:
            stmt = stmt.where(OHLC.source == broker_type)
        if market_type is not None:
            stmt = stmt.where(OHLC.market_type == market_type)
        if timeframe is not None:
            stmt = stmt.where(OHLC.timeframe == timeframe)

        result = await db_sess.execute(stmt)

        rows = result.all()
        has_next = len(rows) > limit
        rows = rows[:limit]

        data = [
            OHLCInfo(
                symbol=row.symbol,
                broker=row.source,
                market_type=row.market_type,
                timeframe=row.timeframe,
                start_date=datetime.fromtimestamp(row.start_ts, UTC),
                end_date=datetime.fromtimestamp(row.end_ts, UTC),
            )
            for row in rows
        ]

        return PaginatedResponse(size=len(data), has_next=has_next, data=data, page=page)

    async def get_symbol_info(self, symbol: str, market_type: MarketType, broker_type: BrokerType, timeframe: Timeframe,
                              db_sess: AsyncSession) -> OHLCInfo:
        res = await db_sess.execute(
            select(
                OHLC.source,
                OHLC.timeframe,
                OHLC.market_type,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
            .where(OHLC.symbol == symbol, OHLC.source == broker_type, OHLC.market_type == market_type,
                   OHLC.timeframe == timeframe)
            .group_by(OHLC.source, OHLC.market_type, OHLC.timeframe)
        )

        row = res.first()

        if row is None:
            raise SymbolNotFoundException(symbol)

        return OHLCInfo(
            symbol=symbol,
            broker=row.source,
            market_type=row.market_type,
            timeframe=row.timeframe,
            start_date=datetime.fromtimestamp(row.start_ts, UTC),
            end_date=datetime.fromtimestamp(row.end_ts, UTC),
        )
