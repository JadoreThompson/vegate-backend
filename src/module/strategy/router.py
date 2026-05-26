from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.dependencies import depends_class, depends_db_sess, depends_jwt
from module.api.schema import PaginatedResponse
from module.backtest import BacktestsService
from module.backtest.schema import BacktestResponse
from module.deployment import DeploymentsService
from module.deployment.schema import StrategyDeploymentResponse
from module.jwt import JWTPayload
from .schema import (
    CreateStrategyRequest,
    StrategyCodeResponse,
    StrategyResponse,
    UpdateStrategyRequest,
)
from .service import StrategyService

router = APIRouter(prefix="/api/v1/strategy", tags=["Strategy"])


@router.post("/", response_model=StrategyResponse, status_code=200)
async def create_strategy(
    body: CreateStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
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
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
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
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
    backtests_service: BacktestsService = Depends(depends_class(BacktestsService)),
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
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
    deployments_service: DeploymentsService = Depends(
        depends_class(DeploymentsService)
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
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
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
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
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


@router.put("/{strategy_id}/code")
async def update_strategy_code(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
    file: UploadFile = File(),
):
    if not file.filename.endswith(".py"):
        raise HTTPException(status_code=400, detail="File must have a .py extension")

    code = (await file.read()).decode()
    strategy = await strategy_service.update_code(strategy_id, jwt.sub, code, db_sess)
    await db_sess.commit()


@router.get("/{strategy_id}/code", response_model=StrategyCodeResponse)
async def get_strategy_code(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
):
    """Get the code of a strategy."""
    strategy = await strategy_service.get_user_strategy(strategy_id, jwt.sub, db_sess)
    return StrategyCodeResponse(code=strategy.code)


@router.patch("/{strategy_id}/code")
async def update_strategy_code(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
    file: UploadFile | None = File(None),
    code: str | None = Form(None),
):
    if file is not None and code is not None:
        raise HTTPException(
            status_code=400, detail="Provide either a file or code, not both"
        )

    if file is None and code is None:
        raise HTTPException(
            status_code=400, detail="Provide either a file (.py) or code"
        )

    if file is not None:
        if not file.filename.endswith(".py"):
            raise HTTPException(
                status_code=400, detail="File must have a .py extension"
            )
        code = (await file.read()).decode()

    strategy = await strategy_service.update_code(strategy_id, jwt.sub, code, db_sess)
    await db_sess.commit()


@router.delete("/{strategy_id}", status_code=204)
async def delete_strategy(
    strategy_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    strategy_service: StrategyService = Depends(depends_class(StrategyService)),
):
    """Delete a strategy."""
    await strategy_service.delete(strategy_id, jwt.sub, db_sess)
    await db_sess.commit()
