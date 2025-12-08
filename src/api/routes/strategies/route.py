from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.typing import JWTPayload
from db_models import Strategies
from .controller import (
    create_strategy,
    get_strategy,
    get_strategy_summary,
    list_strategies,
    list_strategy_summaries,
    update_strategy,
)
from .models import (
    StrategyCreate,
    StrategyDetailResponse,
    StrategyMetrics,
    StrategyResponse,
    StrategySummaryResponse,
    StrategyUpdate,
)


router = APIRouter(prefix="/strategies", tags=["Strategies"])


@router.post("/", response_model=StrategyResponse, status_code=200)
async def create_strategy_endpoint(
    body: StrategyCreate,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    strategy = await create_strategy(jwt.sub, body, db_sess)
    rsp_body = StrategyResponse(
        strategy_id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )
    await db_sess.commit()

    return rsp_body


@router.get("/{strategy_id}", response_model=StrategyDetailResponse)
async def get_strategy_endpoint(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get a strategy by ID with full details including code."""
    strategy = await get_strategy(strategy_id, db_sess)
    if not strategy or strategy.user_id != jwt.sub:
        raise HTTPException(status_code=404, detail="Strategy not found.")

    return StrategyDetailResponse(
        strategy_id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        code=strategy.code,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.get("/", response_model=list[StrategyResponse])
async def list_strategies_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all strategies with pagination (without code field)."""
    strategies = await list_strategies(jwt.sub, db_sess, skip, limit)
    await db_sess.commit()

    return [
        StrategyResponse(
            strategy_id=s.strategy_id,
            name=s.name,
            description=s.description,
            created_at=s.created_at,
            updated_at=s.updated_at,
        )
        for s in strategies
    ]


@router.patch("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy_endpoint(
    strategy_id: UUID,
    body: StrategyUpdate,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Update a strategy (name and/or description only)."""
    strategy = await update_strategy(jwt.sub, strategy_id, body, db_sess)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found.")
    
    rsp_body = StrategyResponse(
        strategy_id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )

    await db_sess.commit()
    return rsp_body


@router.delete("/{strategy_id}", status_code=204)
async def delete_strategy_endpoint(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Delete a strategy."""
    strategy = await get_strategy(strategy_id, db_sess)
    if strategy is None or not strategy.user_id == jwt.sub:
        raise HTTPException(status_code=404, detail="Strategy not found.")

    await db_sess.execute(
        delete(Strategies).where(
            Strategies.strategy_id == strategy_id, Strategies.user_id == jwt.sub
        )
    )

    await db_sess.commit()


@router.get("/{strategy_id}/summary", response_model=StrategySummaryResponse)
async def get_strategy_summary_endpoint(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get a strategy summary with pre-calculated metrics."""
    strategy = await get_strategy_summary(jwt.sub, strategy_id, db_sess)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found.")

    # Extract metrics from JSONB field or use defaults
    metrics_data = strategy.metrics or {}
    metrics = StrategyMetrics(
        realised_pnl=metrics_data.get("realised_pnl", 0.0),
        unrealised_pnl=metrics_data.get("unrealised_pnl", 0.0),
        total_return=metrics_data.get("total_return", 0.0),
        sharpe_ratio=metrics_data.get("sharpe_ratio", 0.0),
        max_drawdown=metrics_data.get("max_drawdown", 0.0),
        equity_curve=metrics_data.get("equity_curve", []),
    )

    return StrategySummaryResponse(
        strategy_id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
        metrics=metrics,
    )


@router.get("/summaries/", response_model=list[StrategySummaryResponse])
async def list_strategy_summaries_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all strategies with pre-calculated metrics summaries."""
    strategies = await list_strategy_summaries(jwt.sub, db_sess, skip, limit)

    results = []

    for s in strategies:
        metrics_data = s.metrics or {}
        
        metrics = StrategyMetrics(
            realised_pnl=metrics_data.get("realised_pnl", 0.0),
            unrealised_pnl=metrics_data.get("unrealised_pnl", 0.0),
            total_return=metrics_data.get("total_return", 0.0),
            sharpe_ratio=metrics_data.get("sharpe_ratio", 0.0),
            max_drawdown=metrics_data.get("max_drawdown", 0.0),
            equity_curve=metrics_data.get("equity_curve", []),
        )
        
        results.append(
            StrategySummaryResponse(
                strategy_id=s.strategy_id,
                name=s.name,
                description=s.description,
                created_at=s.created_at,
                updated_at=s.updated_at,
                metrics=metrics,
            )
        )

    return results
