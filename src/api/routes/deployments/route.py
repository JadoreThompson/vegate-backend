from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.backtest_queue import get_backtest_queue
from api.dependencies import depends_db_sess, depends_jwt
from api.types import JWTPayload
from enums import DeploymentStatus
from infra.db.models import Strategies
from .controller import (
    create_deployment,
    get_deployment_orders,
    get_deployment_with_metrics,
    list_all_deployments,
    list_strategy_deployments,
    stop_deployment,
)
from .models import DeployStrategyRequest, DeploymentResponse, DeploymentDetailResponse
from api.shared.models import OrderResponse, PerformanceMetrics


router = APIRouter(prefix="/deployments", tags=["Deployments"])


@router.post(
    "/strategies/{strategy_id}/deploy",
    response_model=DeploymentResponse,
    status_code=201,
)
async def deploy_strategy_endpoint(
    strategy_id: UUID,
    body: DeployStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Deploy a strategy to a broker connection.

    Creates a new deployment that will run the specified strategy
    with the given configuration on the connected broker account.
    """
    deployment = await create_deployment(jwt.sub, strategy_id, body, db_sess)

    # Push deployment job to the queue
    queue = get_backtest_queue()
    if queue is not None:
        queue.put({"deployment_id": str(deployment.deployment_id)})
    else:
        raise HTTPException(
            status_code=503,
            detail="Deployment queue is not available. Backend service may not be running.",
        )

    rsp_body = DeploymentResponse(
        deployment_id=deployment.deployment_id,
        strategy_id=deployment.strategy_id,
        broker_connection_id=deployment.broker_connection_id,
        symbol=deployment.symbol,
        timeframe=deployment.timeframe,
        status=deployment.status,
        error_message=deployment.error_message,
        created_at=deployment.created_at,
        updated_at=deployment.updated_at,
        stopped_at=deployment.stopped_at,
    )

    await db_sess.commit()

    return rsp_body


@router.get(
    "/strategies/{strategy_id}/deployments",
    response_model=list[DeploymentResponse],
)
async def list_strategy_deployments_endpoint(
    strategy_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all deployments for a specific strategy with pagination.

    Returns list of deployments for the given strategy,
    ordered by creation date (newest first).
    """
    deployments = await list_strategy_deployments(
        jwt.sub, strategy_id, db_sess, skip, limit
    )

    return [
        DeploymentResponse(
            deployment_id=d.deployment_id,
            strategy_id=d.strategy_id,
            broker_connection_id=d.broker_connection_id,
            symbol=d.symbol,
            timeframe=d.timeframe,
            starting_balance=d.starting_balance or 0,
            status=d.status,
            error_message=d.error_message,
            created_at=d.created_at,
            updated_at=d.updated_at,
            stopped_at=d.stopped_at,
        )
        for d in deployments
    ]


@router.get("/{deployment_id}", response_model=DeploymentResponse)
async def get_deployment_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get deployment details by ID.

    Returns full details of a specific deployment including status,
    configuration, and error messages if any.
    """
    # deployment = await get_deployment(deployment_id, db_sess)
    deployment, metrics = await get_deployment_with_metrics(
        jwt.sub, deployment_id, db_sess
    )

    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment not found")

    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )
    if not strategy or strategy.user_id != jwt.sub:
        raise HTTPException(status_code=404, detail="Deployment not found")

    return DeploymentResponse(
        deployment_id=deployment.deployment_id,
        strategy_id=deployment.strategy_id,
        broker_connection_id=deployment.broker_connection_id,
        symbol=deployment.symbol,
        timeframe=deployment.timeframe,
        starting_balance=deployment.starting_balance or 0,
        status=deployment.status,
        error_message=deployment.error_message,
        created_at=deployment.created_at,
        updated_at=deployment.updated_at,
        stopped_at=deployment.stopped_at,
    )


@router.get("/{deployment_id}/details", response_model=DeploymentDetailResponse)
async def get_deployment_details_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get detailed deployment information including performance metrics.

    This endpoint calculates real-time performance metrics from the deployment's
    order history, including:
    - Realised and unrealised PnL
    - Total return percentage
    - Sharpe ratio
    - Maximum drawdown
    - Trade count
    - Equity curve

    Note: Metrics are calculated on-demand from orders for accuracy.
    """
    deployment, metrics = await get_deployment_with_metrics(
        jwt.sub, deployment_id, db_sess
    )

    return DeploymentDetailResponse(
        deployment_id=deployment.deployment_id,
        strategy_id=deployment.strategy_id,
        broker_connection_id=deployment.broker_connection_id,
        symbol=deployment.symbol,
        timeframe=deployment.timeframe,
        starting_balance=deployment.starting_balance or 0,
        status=deployment.status,
        error_message=deployment.error_message,
        created_at=deployment.created_at,
        updated_at=deployment.updated_at,
        stopped_at=deployment.stopped_at,
        metrics=metrics,
    )


@router.post("/{deployment_id}/stop", response_model=DeploymentResponse)
async def stop_deployment_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Stop a running deployment.

    Updates deployment status and sets stopped_at timestamp.
    Can only stop deployments that are currently RUNNING or PENDING.
    """
    deployment = await stop_deployment(jwt.sub, deployment_id, db_sess)
    rsp_body = DeploymentResponse(
        deployment_id=deployment.deployment_id,
        strategy_id=deployment.strategy_id,
        broker_connection_id=deployment.broker_connection_id,
        symbol=deployment.symbol,
        timeframe=deployment.timeframe,
        starting_balance=deployment.starting_balance or 0,
        status=deployment.status,
        error_message=deployment.error_message,
        created_at=deployment.created_at,
        updated_at=deployment.updated_at,
        stopped_at=deployment.stopped_at,
    )
    await db_sess.commit()

    return rsp_body


@router.get("/", response_model=list[DeploymentResponse])
async def list_all_deployments_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    status: DeploymentStatus | None = Query(None),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all user's deployments with pagination and optional status filter.

    Returns list of all deployments belonging to the user,
    optionally filtered by status, ordered by creation date (newest first).
    """
    deployments = await list_all_deployments(jwt.sub, db_sess, skip, limit, status)

    return [
        DeploymentResponse(
            deployment_id=d.deployment_id,
            strategy_id=d.strategy_id,
            broker_connection_id=d.broker_connection_id,
            symbol=d.symbol,
            timeframe=d.timeframe,
            starting_balance=d.starting_balance or 0,
            status=d.status,
            error_message=d.error_message,
            created_at=d.created_at,
            updated_at=d.updated_at,
            stopped_at=d.stopped_at,
        )
        for d in deployments
    ]


@router.get("/{deployment_id}/orders", response_model=list[OrderResponse])
async def get_deployment_orders_endpoint(
    deployment_id: UUID,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get all orders/trades for a deployment with pagination.

    Returns orders ordered by submission time (oldest first).
    """
    orders = await get_deployment_orders(jwt.sub, deployment_id, db_sess, skip, limit)

    return [
        OrderResponse(
            order_id=o.order_id,
            symbol=o.symbol,
            side=o.side,
            order_type=o.order_type,
            quantity=o.quantity,
            notional=o.notional,
            filled_quantity=o.filled_quantity,
            limit_price=o.limit_price,
            stop_price=o.stop_price,
            average_fill_price=o.avg_fill_price,
            status=o.status,
            submitted_at=o.submitted_at,
            filled_at=o.filled_at,
            broker_order_id=o.broker_order_id,
        )
        for o in orders
    ]
