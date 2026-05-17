from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.routes.strategy.models import (
    CreateStrategyRequest,
    StrategyDetailResponse,
    StrategyResponse,
    UpdateStrategyRequest,
)
from api.routes.strategy.service import StrategyService
from api.types import JWTPayload

router = APIRouter(prefix="/strategy", tags=["Strategy"])
strategy_service = StrategyService()


@router.post("/", response_model=StrategyResponse, status_code=200)
async def create_strategy_endpoint(
    body: CreateStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    strategy = await strategy_service.create(body, jwt.sub, db_sess)
    await db_sess.commit()
    return StrategyResponse(
        id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        prompt=strategy.prompt,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.get("/{strategy_id}", response_model=StrategyDetailResponse)
async def get_strategy_endpoint(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Get a strategy by ID with full details including code."""

    strategy = await strategy_service.get_strategy(strategy_id, jwt.sub, db_sess)

    return StrategyDetailResponse(
        strategy_id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        code=strategy.code,
        prompt=strategy.prompt,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.get("/", response_model=list[StrategyResponse])
async def list_strategies_endpoint(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """List all strategy with pagination (without code field)."""
    return await strategy_service.get_strategies(
        jwt.sub, db_sess, page=page, limit=limit
    )


@router.patch("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy_endpoint(
    strategy_id: UUID,
    body: UpdateStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Update a strategy (name and/or description only)."""
    strategy = await strategy_service.update(body, strategy_id, jwt.sub, db_sess)
    await db_sess.commit()
    return StrategyResponse(
        id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        prompt=strategy.prompt,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.delete("/{strategy_id}", status_code=204)
async def delete_strategy_endpoint(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """Delete a strategy."""
    await strategy_service.delete(strategy_id, jwt.sub, db_sess)
    await db_sess.commit()
