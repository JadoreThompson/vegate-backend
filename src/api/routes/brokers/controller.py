from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infra.db.models import BrokerConnections, StrategyDeployments


async def get_broker_connection(
    connection_id: UUID, db_sess: AsyncSession
) -> BrokerConnections | None:
    """Get a broker connection by ID."""
    return await db_sess.scalar(
        select(BrokerConnections).where(
            BrokerConnections.connection_id == connection_id
        )
    )


async def list_broker_connections(
    user_id: UUID,
    db_sess: AsyncSession,
) -> list[BrokerConnections]:
    """
    List all broker connections for a user.

    Returns connections ordered by broker type.
    """
    result = await db_sess.execute(
        select(BrokerConnections)
        .where(BrokerConnections.user_id == user_id)
        .order_by(BrokerConnections.broker)
    )
    return list(result.scalars().all())


async def delete_broker_connection(
    user_id: UUID,
    connection_id: UUID,
    db_sess: AsyncSession,
) -> bool:
    """
    Delete a broker connection.

    Verifies that the connection belongs to the user before deletion.
    Returns True if deleted, False if not found.
    """
    connection = await get_broker_connection(connection_id, db_sess)
    if not connection:
        return False

    # Verify ownership
    if connection.user_id != user_id:
        return False

    # Check if there are active deployments using this connection
    active_deployments = await db_sess.scalar(
        select(StrategyDeployments)
        .where(
            StrategyDeployments.broker_connection_id == connection_id,
            StrategyDeployments.status.in_(["pending", "running"]),
        )
        .limit(1)
    )

    if active_deployments:
        raise HTTPException(
            400,
            "Cannot delete broker connection with active deployments. Stop all deployments first.",
        )

    await db_sess.delete(connection)
    return True
