from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import CSVQuery, depends_db_sess, depends_jwt
from api.shared.models import OrderResponse
from api.types import JWTPayload
from enums import BacktestStatus
from infra.db.model import Strategy, BacktestMetrics
from service.backtest import ProcessBacktestService
from service.railway import RailwayService
from .controller import (
    create_backtest,
    delete_backtest,
    get_backtest,
    get_backtest_orders,
    list_backtests,
)
from .model import (
    CreateBacktestRequest,
    BacktestResponse, CreateBacktestResponse, BacktestOrderResponse,
)
from api.routes.strategy.service import StrategyService
from .service import BacktestService

# from ..strategy.service import StrategyService

router = APIRouter(prefix="/backtests", tags=["Backtests"])
# railway_service = RailwayService()
backtest_service = BacktestService(strategy_service=StrategyService(), backtest_service=ProcessBacktestService())


@router.post("/", response_model=CreateBacktestResponse, status_code=201)
async def create_backtest_endpoint(
    body: CreateBacktestRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Create a new backtest."""
    # backtest = await create_backtest(jwt.sub, body, db_sess)
    #
    # rsp_body = BacktestResponse(
    #     backtest_id=backtest.id,
    #     strategy_id=backtest.strategy_id,
    #     symbol=backtest.symbol,
    #     starting_balance=backtest.starting_balance,
    #     status=backtest.status,
    #     created_at=backtest.created_at,
    # )
    #
    # # deployment_data = await railway_service.deploy(backtest_id=backtest.id)
    # # backtest.server_data = deployment_data
    # await db_sess.commit()
    #
    # return rsp_body
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
    # backtest = await get_backtest(backtest_id, db_sess)
    # if not backtest:
    #     raise HTTPException(status_code=404, detail="Backtest not found")
    #
    # metrics = await db_sess.scalar(
    #     select(BacktestMetrics).where(BacktestMetrics.backtest_id == backtest_id)
    # )
    # if metrics is None:
    #     raise HTTPException(status_code=404, detail="Backtest metrics not found")
    #
    # strategy = await db_sess.scalar(
    #     select(Strategy).where(Strategy.strategy_id == backtest.strategy_id)
    # )
    # if not strategy or strategy.user_id != jwt.sub:
    #     raise HTTPException(status_code=404, detail="Backtest not found")
    #
    # return BacktestDetailResponse(
    #     backtest_id=backtest.id,
    #     strategy_id=backtest.strategy_id,
    #     symbol=backtest.symbol,
    #     starting_balance=backtest.starting_balance,
    #     status=backtest.status,
    #     created_at=backtest.created_at,
    #     metrics=metrics,
    # )
    return await backtest_service.get_backtest(backtest_id, jwt.sub, db_sess)


@router.get("/", response_model=list[BacktestResponse])
async def list_backtests_endpoint(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    status: list[BacktestStatus] | None = CSVQuery("status", BacktestStatus, None),
    symbols: list[str] | None = CSVQuery("symbols", str, None),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all backtests with pagination."""
    # backtests = await list_backtests(jwt.sub, db_sess, status, symbols, page, limit)
    # res = [
    #     BacktestResponse(
    #         backtest_id=b.id,
    #         strategy_id=b.strategy_id,
    #         symbol=b.symbol,
    #         starting_balance=b.starting_balance,
    #         status=b.status,
    #         created_at=b.created_at,
    #     )
    #     for b in backtests
    # ]
    #
    # return res
    return await backtest_service.get_backtests(jwt.sub, db_sess, page=page, limit=limit, status=status, symbols=symbols)


@router.delete("/{backtest_id}", status_code=204)
async def delete_backtest_endpoint(
    backtest_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Delete a backtest."""
    # deleted = await delete_backtest(jwt.sub, backtest_id, db_sess)
    # if not deleted:
    #     raise HTTPException(status_code=404, detail="Backtest not found")
    #
    # await db_sess.commit()
    await backtest_service.delete(backtest_id, jwt.sub, db_sess)



@router.get("/{backtest_id}/orders", response_model=list[BacktestOrderResponse])
async def get_backtest_orders_endpoint(
    backtest_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get all orders/trades for a backtest with pagination."""
    # orders = await get_backtest_orders(jwt.sub, backtest_id, db_sess, page, limit)
    # rsp_body = [
    #     OrderResponse(
    #         order_id=o.order_id,
    #         symbol=o.symbol,
    #         side=o.side,
    #         order_type=o.order_type,
    #         quantity=o.quantity,
    #         filled_quantity=o.filled_quantity,
    #         limit_price=o.limit_price,
    #         stop_price=o.stop_price,
    #         average_fill_price=o.avg_fill_price,
    #         status=o.status,
    #         time_in_force=o.time_in_force,
    #         submitted_at=o.submitted_at,
    #         filled_at=o.filled_at,
    #         client_order_id=o.client_order_id,
    #         broker_order_id=o.broker_order_id,
    #     )
    #     for o in orders
    # ]
    #
    # await db_sess.commit()
    #
    # return rsp_body
    return await backtest_service.get_orders(backtest_id, jwt.sub, db_sess, page=page, limit=limit)
