from datetime import datetime, UTC

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.markets.model import OHLCInfo
from infra.db.model import OHLC


class MarketsService:

    def __init__(self):
        pass

    async def get_symbols_info(self, db_sess: AsyncSession, page: int, limit: int) -> PaginatedResponse[OHLCInfo]:
        offset = (page - 1) * limit

        result = await db_sess.execute(
            select(
                OHLC.source,
                OHLC.symbol,
                OHLC.market_type,
                OHLC.timeframe,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
            .group_by(
                OHLC.source,
                OHLC.symbol,
                OHLC.timeframe,
                OHLC.market_type
            )
            .order_by(
                OHLC.symbol,
                OHLC.source,
                OHLC.timeframe,
            )
            .limit(limit + 1)
            .offset(offset)
        )

        rows = result.scalars().all()
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

    async def get_symbol_info(self, symbol: str, db_sess: AsyncSession) -> list[OHLCInfo]:
        result = await db_sess.execute(
            select(
                OHLC.source,
                OHLC.timeframe,
                OHLC.market_type,
                func.min(OHLC.timestamp).label("start_ts"),
                func.max(OHLC.timestamp).label("end_ts"),
            )
            .where(OHLC.symbol == symbol)
            .group_by(OHLC.source, OHLC.timeframe, OHLC.market_type)
        )

        rows = result.all()

        return [
            OHLCInfo(
                symbol=symbol,
                broker=row.source,
                market_type=row.market_type,
                timeframe=row.timeframe,
                start_date=datetime.fromtimestamp(row.start_ts, UTC),
                end_date=datetime.fromtimestamp(row.end_ts, UTC),
            )
            for row in rows
        ]