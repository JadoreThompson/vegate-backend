from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession

from module.api.dependencies import depends_class, depends_db_sess
from module.api.schema import PaginatedResponse
from module.broker.enums import BrokerType
from .enums import MarketType, Timeframe
from .schema import InstrumentInfo, OHLC as OHLCResponse
from .service import MarketsService

router = APIRouter(prefix="/api/v1/markets", tags=["markets"])


@router.get("/info", response_model=PaginatedResponse[InstrumentInfo])
async def get_all_symbols_info(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    symbol: str | None = None,
    db_sess: AsyncSession = Depends(depends_db_sess),
    markets_service: MarketsService = Depends(depends_class(MarketsService)),
):
    return await markets_service.get_symbols_info(
        db_sess, page=page, limit=limit, symbol=symbol
    )


@router.get("/bars", response_model=PaginatedResponse[OHLCResponse])
async def get_ohlc_bars(
    symbol: str = Query(...),
    market_type: MarketType = Query(...),
    broker_type: BrokerType = Query(...),
    timeframe: Timeframe = Query(...),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    start_time: int | None = None,
    end_time: int | None = None,
    db_sess: AsyncSession = Depends(depends_db_sess),
    markets_service: MarketsService = Depends(depends_class(MarketsService)),
):
    return await markets_service.get_ohlc_bars(
        db_sess,
        symbol=symbol,
        market_type=market_type,
        broker_type=broker_type,
        timeframe=timeframe,
        page=page,
        limit=limit,
        start_time=start_time,
        end_time=end_time,
    )
