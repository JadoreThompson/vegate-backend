from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.typing import JWTPayload
from db_models import Strategies
from .controller import (
    create_backtest,
    delete_backtest,
    get_backtest,
    get_backtest_orders,
    list_backtests,
    update_backtest,
)
from .models import (
    BacktestCreate,
    BacktestDetailResponse,
    BacktestMetrics,
    BacktestResponse,
    BacktestUpdate,
    OrderResponse,
)


router = APIRouter(prefix="/backtests", tags=["Backtests"])


@router.post("/", response_model=BacktestResponse, status_code=201)
async def create_backtest_endpoint(
    body: BacktestCreate,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Create a new backtest."""
    backtest = await create_backtest(jwt.sub, body, db_sess)

    rsp_body = BacktestResponse(
        backtest_id=backtest.backtest_id,
        strategy_id=backtest.strategy_id,
        ticker=backtest.ticker,
        starting_balance=backtest.starting_balance,
        status=backtest.status,
        created_at=backtest.created_at,
    )
    await db_sess.commit()

    return rsp_body


@router.get("/{backtest_id}", response_model=BacktestDetailResponse)
async def get_backtest_endpoint(
    backtest_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get a backtest by ID with full details including metrics."""
    backtest = await get_backtest(backtest_id, db_sess)
    if not backtest:
        raise HTTPException(status_code=404, detail="Backtest not found")

    # Verify ownership through strategy relationshi

    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == backtest.strategy_id)
    )
    if not strategy or strategy.user_id != jwt.sub:
        raise HTTPException(status_code=404, detail="Backtest not found")

    # Extract metrics from JSONB field
    metrics = None
    if backtest.metrics:
        metrics = BacktestMetrics(
            realised_pnl=backtest.metrics.get("realised_pnl", 0.0),
            unrealised_pnl=backtest.metrics.get("unrealised_pnl", 0.0),
            total_return=backtest.metrics.get("total_return", 0.0),
            sharpe_ratio=backtest.metrics.get("sharpe_ratio", 0.0),
            max_drawdown=backtest.metrics.get("max_drawdown", 0.0),
            win_rate=backtest.metrics.get("win_rate", 0.0),
            total_trades=backtest.metrics.get("total_trades", 0),
        )

    return BacktestDetailResponse(
        backtest_id=backtest.backtest_id,
        strategy_id=backtest.strategy_id,
        ticker=backtest.ticker,
        starting_balance=backtest.starting_balance,
        status=backtest.status,
        created_at=backtest.created_at,
        metrics=metrics,
    )


@router.get("/", response_model=list[BacktestResponse])
async def list_backtests_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all backtests with pagination."""
    backtests = await list_backtests(jwt.sub, db_sess, skip, limit)
    await db_sess.commit()

    return [
        BacktestResponse(
            backtest_id=b.backtest_id,
            strategy_id=b.strategy_id,
            ticker=b.ticker,
            starting_balance=b.starting_balance,
            status=b.status,
            created_at=b.created_at,
        )
        for b in backtests
    ]


@router.patch("/{backtest_id}", response_model=BacktestResponse)
async def update_backtest_endpoint(
    backtest_id: UUID,
    body: BacktestUpdate,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Update a backtest (status and/or metrics)."""
    backtest = await update_backtest(jwt.sub, backtest_id, body, db_sess)
    if not backtest:
        raise HTTPException(status_code=404, detail="Backtest not found")

    await db_sess.commit()

    return BacktestResponse(
        backtest_id=backtest.backtest_id,
        strategy_id=backtest.strategy_id,
        ticker=backtest.ticker,
        starting_balance=backtest.starting_balance,
        status=backtest.status,
        created_at=backtest.created_at,
    )


@router.delete("/{backtest_id}", status_code=204)
async def delete_backtest_endpoint(
    backtest_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Delete a backtest."""
    deleted = await delete_backtest(jwt.sub, backtest_id, db_sess)
    if not deleted:
        raise HTTPException(status_code=404, detail="Backtest not found")

    await db_sess.commit()


@router.get("/{backtest_id}/orders", response_model=list[OrderResponse])
async def get_backtest_orders_endpoint(
    backtest_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get all orders/trades for a backtest with pagination."""
    orders = await get_backtest_orders(jwt.sub, backtest_id, db_sess, skip, limit)
    await db_sess.commit()

    return [
        OrderResponse(
            order_id=o.order_id,
            symbol=o.symbol,
            side=o.side,
            order_type=o.order_type,
            quantity=o.quantity,
            filled_quantity=o.filled_quantity,
            limit_price=o.limit_price,
            stop_price=o.stop_price,
            average_fill_price=o.average_fill_price,
            status=o.status,
            time_in_force=o.time_in_force,
            submitted_at=o.submitted_at,
            filled_at=o.filled_at,
            client_order_id=o.client_order_id,
            broker_order_id=o.broker_order_id,
        )
        for o in orders
    ]
