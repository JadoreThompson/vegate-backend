from datetime import datetime
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.enums import StrategyDeploymentStatus
from db_models import BrokerConnections, Orders, Strategies, StrategyDeployments
from utils.utils import get_datetime
from .exc import DeploymentAlreadyStoppedError, InvalidDeploymentStatusError
from .models import DeployStrategyRequest


async def create_deployment(
    user_id: UUID,
    strategy_id: UUID,
    data: DeployStrategyRequest,
    db_sess: AsyncSession,
) -> StrategyDeployments:
    """
    Create a new deployment for a strategy.

    Verifies that both the strategy and broker connection exist and belong to the user,
    then creates a new deployment record with PENDING status.
    """
    # Verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id,
            Strategies.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    # Verify the broker connection exists and belongs to the user
    broker_connection = await db_sess.scalar(
        select(BrokerConnections).where(
            BrokerConnections.connection_id == data.broker_connection_id,
            BrokerConnections.user_id == user_id,
        )
    )
    if not broker_connection:
        raise HTTPException(404, "Broker connection not found")

    # Create the deployment
    new_deployment = StrategyDeployments(
        strategy_id=strategy_id,
        broker_connection_id=data.broker_connection_id,
        symbol=data.symbol,
        timeframe=data.timeframe,
        status=StrategyDeploymentStatus.PENDING,
    )

    db_sess.add(new_deployment)
    await db_sess.flush()
    await db_sess.refresh(new_deployment)

    return new_deployment


async def get_deployment(
    deployment_id: UUID, db_sess: AsyncSession
) -> StrategyDeployments | None:
    """Get a deployment by ID."""
    return await db_sess.scalar(
        select(StrategyDeployments).where(
            StrategyDeployments.deployment_id == deployment_id
        )
    )


async def list_strategy_deployments(
    user_id: UUID,
    strategy_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> list[StrategyDeployments]:
    """
    List all deployments for a specific strategy with pagination.

    Returns deployments ordered by creation date (newest first).
    """
    # First verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id,
            Strategies.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    # Get deployments for this strategy
    result = await db_sess.execute(
        select(StrategyDeployments)
        .where(StrategyDeployments.strategy_id == strategy_id)
        .offset(offset)
        .limit(limit)
        .order_by(StrategyDeployments.created_at.desc())
    )
    return list(result.scalars().all())


async def list_all_deployments(
    user_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
    status: StrategyDeploymentStatus | None = None,
) -> list[StrategyDeployments]:
    """
    List all deployments for a user with optional status filter and pagination.

    Returns deployments ordered by creation date (newest first).
    """
    query = (
        select(StrategyDeployments)
        .join(Strategies)
        .where(Strategies.user_id == user_id)
    )

    # Apply status filter if provided
    if status is not None:
        query = query.where(StrategyDeployments.status == status)

    query = (
        query.offset(offset)
        .limit(limit)
        .order_by(StrategyDeployments.created_at.desc())
    )

    result = await db_sess.execute(query)
    return list(result.scalars().all())


async def stop_deployment(
    user_id: UUID,
    deployment_id: UUID,
    db_sess: AsyncSession,
) -> StrategyDeployments:
    """
    Stop a running deployment.

    Updates the deployment status to STOPPED and sets the stopped_at timestamp.
    Can only stop deployments that are currently RUNNING or PENDING.
    """
    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(404, "Deployment not found")

    # Verify ownership through strategy relationship
    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Deployment not found")

    # Check if deployment is already stopped
    if deployment.status == StrategyDeploymentStatus.STOPPED:
        raise HTTPException(400, "Deployment is already stopped")

    # Check if deployment is in ERROR state
    if deployment.status == StrategyDeploymentStatus.ERROR:
        raise HTTPException(400, "Cannot stop a deployment in ERROR state")

    # Update deployment status
    deployment.status = StrategyDeploymentStatus.STOPPED
    deployment.stopped_at = get_datetime()

    await db_sess.flush()
    await db_sess.refresh(deployment)

    return deployment


async def get_deployment_orders(
    user_id: UUID,
    deployment_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> list[Orders]:
    """
    Get all orders for a deployment with pagination.

    Returns orders ordered by submission time (oldest first).
    """
    # First verify the deployment exists and user has access
    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(404, "Deployment not found")

    # Verify ownership through strategy relationship
    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Deployment not found")

    # Get orders for this deployment
    result = await db_sess.execute(
        select(Orders)
        .where(Orders.deployment_id == deployment_id)
        .offset(offset)
        .limit(limit)
        .order_by(Orders.submitted_at.asc())
    )
    return list(result.scalars().all())
