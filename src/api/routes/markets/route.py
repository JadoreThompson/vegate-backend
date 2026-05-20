from fastapi import APIRouter, Depends, Query

from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_class, depends_db_sess
from api.models import PaginatedResponse
from api.routes.markets.model import InstrumentInfo
from api.routes.markets.service import MarketsService

router = APIRouter(prefix="/markets", tags=["markets"])


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
