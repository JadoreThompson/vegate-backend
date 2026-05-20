from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_class, depends_db_sess, depends_jwt
from api.models import PaginatedResponse
from api.routes.backtests.model import BacktestResponse
from api.routes.backtests.service import APIBacktestsService
from api.routes.deployments.models import StrategyDeploymentResponse
from api.routes.deployments.service import APIDeploymentsService
from api.routes.strategy.models import (
    CreateStrategyRequest,
    StrategyResponse,
    UpdateStrategyRequest,
)
from api.routes.strategy.service import APIStrategyService
from api.types import JWTPayload

router = APIRouter(prefix="/strategy", tags=["Strategy"])


@router.post("/", response_model=StrategyResponse, status_code=200)
async def create_strategy(
    body: CreateStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
):
    strategy = await strategy_service.create(body, jwt.sub, db_sess)
    await db_sess.commit()
    return StrategyResponse(
        id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        prompt=strategy.prompt,
        code=strategy.code,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.get("/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
):
    """Get a strategy by ID with full details including code."""

    strategy = await strategy_service.get_strategy(strategy_id, jwt.sub, db_sess)

    return StrategyResponse(
        id=strategy.strategy_id,
        name=strategy.name,
        description=strategy.description,
        code=strategy.code,
        prompt=strategy.prompt,
        created_at=strategy.created_at,
        updated_at=strategy.updated_at,
    )


@router.get(
    "/{strategy_id}/backtests",
    response_model=PaginatedResponse[BacktestResponse],
)
async def get_strategy_backtests(
    strategy_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
    backtests_service: APIBacktestsService = Depends(
        depends_class(APIBacktestsService)
    ),
):
    await strategy_service.get_user_strategy(strategy_id, jwt.sub, db_sess)
    return await backtests_service.get_by_strategy_id(
        strategy_id, db_sess, page=page, limit=limit
    )


@router.get(
    "/{strategy_id}/deployments",
    response_model=PaginatedResponse[StrategyDeploymentResponse],
)
async def get_strategy_deployments(
    strategy_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    await strategy_service.get_user_strategy(strategy_id, jwt.sub, db_sess)
    return await deployments_service.get_by_strategy_id(
        strategy_id, db_sess, page=page, limit=limit
    )


@router.get("/", response_model=PaginatedResponse[StrategyResponse])
async def list_strategies(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    name: str | None = None,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
):
    """List all strategy with pagination (without code field)."""
    return await strategy_service.get_strategies(
        jwt.sub, db_sess, page=page, limit=limit, name=name
    )


@router.patch("/{strategy_id}", response_model=StrategyResponse)
async def update_strategy(
    strategy_id: UUID,
    body: UpdateStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
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
async def delete_strategy(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: APIStrategyService = Depends(depends_class(APIStrategyService)),
):
    """Delete a strategy."""
    await strategy_service.delete(strategy_id, jwt.sub, db_sess)
    await db_sess.commit()
