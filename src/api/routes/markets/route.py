from datetime import UTC, datetime
from fastapi import APIRouter, Depends, Query

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess
from api.models import PaginatedResponse
from api.routes.markets.models import OHLCInfo
from enums import BrokerType
from infra.db.model.ohlc import OHLC

router = APIRouter(prefix="/markets", tags=["markets"])
PAGE_SIZE = 50


@router.get("/info", response_model=PaginatedResponse[OHLCInfo])
async def get_all_symbols_info(
    page: int = Query(default=1, ge=1),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    offset = (page - 1) * PAGE_SIZE

    result = await db_sess.execute(
        select(
            OHLC.source,
            OHLC.symbol,
            OHLC.timeframe,
            func.min(OHLC.timestamp).label("start_ts"),
            func.max(OHLC.timestamp).label("end_ts"),
        )
        .group_by(
            OHLC.source,
            OHLC.symbol,
            OHLC.timeframe,
        )
        .order_by(
            OHLC.symbol,
            OHLC.source,
            OHLC.timeframe,
        )
        .limit(PAGE_SIZE + 1)
        .offset(offset)
    )

    rows = result.all()

    has_next = len(rows) > PAGE_SIZE
    rows = rows[:PAGE_SIZE]

    data = [
        {
            "symbol": row.symbol,
            "broker": BrokerType(row.source),
            "timeframe": row.timeframe,
            "start_date": datetime.fromtimestamp(row.start_ts, UTC),
            "end_date": datetime.fromtimestamp(row.end_ts, UTC),
        }
        for row in rows
    ]

    return PaginatedResponse(size=len(data), has_next=has_next, data=data, page=page)


@router.get("/{symbol}/info", response_model=list[OHLCInfo])
async def get_symbol_info(
    symbol: str,
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    result = await db_sess.execute(
        select(
            OHLC.source,
            OHLC.timeframe,
            func.min(OHLC.timestamp).label("start_ts"),
            func.max(OHLC.timestamp).label("end_ts"),
        )
        .where(OHLC.symbol == symbol)
        .group_by(OHLC.source, OHLC.timeframe)
    )

    rows = result.all()

    return [
        OHLCInfo(
            symbol=symbol,
            broker=BrokerType(row.source),
            timeframe=row.timeframe,
            start_date=datetime.fromtimestamp(row.start_ts, UTC),
            end_date=datetime.fromtimestamp(row.end_ts, UTC),
        )
        for row in rows
    ]
