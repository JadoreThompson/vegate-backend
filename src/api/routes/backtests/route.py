from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import CSVQuery, depends_db_sess, depends_jwt
from api.types import JWTPayload
from api.routes.strategy.service import StrategyService
from api.routes.backtests.model import (
    BacktestOrderResponse,
    BacktestResponse,
    CreateBacktestRequest,
    CreateBacktestResponse,
)
from api.routes.backtests.service import BacktestService
from api.routes.markets.service import MarketsService
from api.models import PaginatedResponse
from enums import BacktestStatus
from service.backtest import ProcessBacktestService

router = APIRouter(prefix="/backtests", tags=["Backtests"])

backtest_service = BacktestService(
    strategy_service=StrategyService(),
    backtest_service=ProcessBacktestService(),
    markets_service=MarketsService(),
)


@router.post("/", response_model=CreateBacktestResponse, status_code=201)
async def create_backtest_endpoint(
        body: CreateBacktestRequest,
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
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
):
    """Get a backtest by ID with full details including metrics."""
    return await backtest_service.get_backtest(backtest_id, jwt.sub, db_sess)


@router.get("/", response_model=PaginatedResponse[BacktestResponse])
async def list_backtests_endpoint(
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        status: list[BacktestStatus] | None = CSVQuery(
            "status",
            BacktestStatus,
            None,
        ),
        symbols: list[str] | None = CSVQuery("symbols", str, None),
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all backtests with pagination."""
    return await backtest_service.get_backtests(
        jwt.sub,
        db_sess,
        page=page,
        limit=limit,
        status=status,
        symbols=symbols,
    )


@router.delete("/{backtest_id}", status_code=204)
async def delete_backtest_endpoint(
        backtest_id: UUID,
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
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
):
    """Get all orders/trades for a backtest with pagination."""
    return await backtest_service.get_orders(
        backtest_id,
        jwt.sub,
        db_sess,
        page=page,
        limit=limit,
    )
