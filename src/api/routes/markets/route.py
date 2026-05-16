from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess
from api.models import PaginatedResponse
from api.routes.markets.model import OHLCInfo
from api.routes.markets.service import MarketsService

router = APIRouter(prefix="/markets", tags=["markets"])

markets_service = MarketsService()


@router.get("/info", response_model=PaginatedResponse[OHLCInfo])
async def get_all_symbols_info(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    return await markets_service.get_symbols_info(db_sess, page, limit)


@router.get("/{symbol}/info", response_model=list[OHLCInfo])
async def get_symbol_info(
    symbol: str,
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    return await markets_service.get_symbol_info(symbol, db_sess)
