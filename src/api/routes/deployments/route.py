from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.typing import JWTPayload
from core.enums import StrategyDeploymentStatus
from services import DeploymentService
from .controller import (
    create_deployment,
    get_deployment,
    get_deployment_orders,
    list_all_deployments,
    list_strategy_deployments,
    stop_deployment,
)
from .models import DeployStrategyRequest, DeploymentResponse
from api.routes.backtests.models import OrderResponse

router = APIRouter(prefix="/deployments", tags=["Deployments"])
deployment_service = DeploymentService()


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

    # Deploy the strategy using the deployment service
    deployment_data = await deployment_service.deploy(
        deployment_id=deployment.deployment_id
    )
    deployment.server_data = deployment_data
    
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
    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment not found")

    # Verify ownership through strategy relationship
    from sqlalchemy import select
    from db_models import Strategies

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
    await db_sess.commit()

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


@router.get("/", response_model=list[DeploymentResponse])
async def list_all_deployments_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    status: StrategyDeploymentStatus | None = Query(None),
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
            filled_quantity=o.filled_quantity,
            limit_price=o.limit_price,
            stop_price=o.stop_price,
            average_fill_price=o.avg_fill_price,
            status=o.status,
            time_in_force=o.time_in_force,
            submitted_at=o.submitted_at,
            filled_at=o.filled_at,
            client_order_id=o.client_order_id,
            broker_order_id=o.broker_order_id,
        )
        for o in orders
    ]
