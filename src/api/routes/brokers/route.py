from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.types import JWTPayload
from .controller import (
    delete_broker_connection,
    get_broker_connection,
    list_broker_connections,
)
from .models import BrokerConnectionResponse

router = APIRouter(prefix="/brokers", tags=["Brokers"])


@router.get("/connections", response_model=list[BrokerConnectionResponse])
async def list_broker_connections_endpoint(
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all broker connections for the authenticated user.

    Returns a list of all broker connections the user has set up,
    including connection IDs, broker types, and account IDs.
    """
    connections = await list_broker_connections(jwt.sub, db_sess)
    return [
        BrokerConnectionResponse(
            connection_id=c.connection_id,
            broker=c.broker,
            broker_account_id=c.broker_account_id,
        )
        for c in connections
    ]


@router.get("/connections/{connection_id}", response_model=BrokerConnectionResponse)
async def get_broker_connection_endpoint(
    connection_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get details of a specific broker connection.

    Returns the connection details if it exists and belongs to the user.
    """
    connection = await get_broker_connection(connection_id, db_sess)
    if not connection:
        raise HTTPException(status_code=404, detail="Broker connection not found")

    # Verify ownership
    if connection.user_id != jwt.sub:
        raise HTTPException(status_code=404, detail="Broker connection not found")

    return BrokerConnectionResponse(
        connection_id=connection.connection_id,
        broker=connection.broker,
        broker_account_id=connection.broker_account_id,
        created_at=None,
    )


@router.delete("/connections/{connection_id}", status_code=204)
async def delete_broker_connection_endpoint(
    connection_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Delete a broker connection.

    Removes the broker connection if it exists and belongs to the user.
    Cannot delete connections that have active deployments.
    """
    deleted = await delete_broker_connection(jwt.sub, connection_id, db_sess)
    if not deleted:
        raise HTTPException(status_code=404, detail="Broker connection not found")

    await db_sess.commit()
