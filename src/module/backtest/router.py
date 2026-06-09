from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.dependencies import (
    CSVQuery,
    depends_class,
    depends_db_sess,
    depends_jwt,
)
from module.api.schema import PaginatedResponse
from module.backtest.enums import BacktestStatus
from module.jwt import JWTPayload
from .schema import (
    BacktestOrderResponse,
    BacktestResponse,
    CreateBacktestRequest,
    CreateBacktestResponse,
)
from .service import BacktestsService

router = APIRouter(prefix="/api/v1/backtests", tags=["Backtests"])


@router.post("/", response_model=CreateBacktestResponse, status_code=201)
async def create_backtest_endpoint(
    body: CreateBacktestRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    backtest_service: BacktestsService = Depends(depends_class(BacktestsService)),
):
    """Create a new backtest."""
    backtest = await backtest_service.create(body, jwt.sub, db_sess)
    await db_sess.commit()

    return {"id": backtest.id}


@router.get("/{backtest_id}", response_model=BacktestResponse)
async def get_backtest_endpoint(
    backtest_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    backtest_service: BacktestsService = Depends(depends_class(BacktestsService)),
):
    """Get a backtest by ID with full details including metrics."""
    return await backtest_service.get_backtest(backtest_id, jwt.sub, db_sess)


@router.delete("/{backtest_id}", status_code=204)
async def delete_backtest_endpoint(
    backtest_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    backtest_service: BacktestsService = Depends(depends_class(BacktestsService)),
):
    """Delete a backtest."""
    await backtest_service.delete(backtest_id, jwt.sub, db_sess)
    await db_sess.commit()


@router.get(
    "/{backtest_id}/orders",
    response_model=PaginatedResponse[BacktestOrderResponse],
)
async def get_backtest_orders_endpoint(
    backtest_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    backtest_service: BacktestsService = Depends(depends_class(BacktestsService)),
):
    """Get all orders/trades for a backtest with pagination."""
    return await backtest_service.get_orders(
        backtest_id,
        jwt.sub,
        db_sess,
        page=page,
        limit=limit,
    )
