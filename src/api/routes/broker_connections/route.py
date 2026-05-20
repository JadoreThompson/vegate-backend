import logging
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.models import PaginatedResponse
from api.routes.broker_connections.service import BrokerConnectionsService
from api.types import JWTPayload
from config import FRONTEND_DOMAIN, FRONTEND_SUB_DOMAIN, SCHEME
from enums import BrokerType
from service.oauth.alpaca import AlpacaService
from .model import (
    BrokerConnectionResponse,
    CreateBrokerConnectionRequest,
    GetOauthUrlResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/broker-connections", tags=["Broker Connections"])
alpaca_oauth_service = AlpacaService()
broker_connections_service = BrokerConnectionsService()


@router.post("", response_model=BrokerConnectionResponse)
async def create_broker_connection(
    body: CreateBrokerConnectionRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    broker_connection = await broker_connections_service.create_broker_connection(
        request=body, user_id=jwt.sub, db_sess=db_sess
    )
    await db_sess.commit()

    return BrokerConnectionResponse(
        id=broker_connection.connection_id,
        broker=broker_connection.broker,
        account_id=broker_connection.broker_account_id,
        account_number=broker_connection.broker_account_number
    )


@router.get("", response_model=PaginatedResponse[BrokerConnectionResponse])
async def list_broker_connections_endpoint(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, ),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all broker connections for the authenticated user.

    Returns a list of all broker connections the user has set up,
    including connection IDs, broker types, and account IDs.
    """
    return await broker_connections_service.get_broker_connections(
        user_id=jwt.sub, db_sess=db_sess, page=page, limit=limit
    )


@router.get("/{connection_id}", response_model=BrokerConnectionResponse)
async def get_broker_connection_endpoint(
    connection_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get details of a specific broker connection.

    Returns the connection details if it exists and belongs to the user.
    """
    broker_conn = await broker_connections_service.get_broker_connection(
        id=connection_id, user_id=jwt.sub, db_sess=db_sess
    )
    return BrokerConnectionResponse(
        id=connection_id,
        broker=broker_conn.broker,
        account_id=broker_conn.broker_account_id,
        account_number=broker_conn.broker_account_number,
    )


@router.delete("/{connection_id}", status_code=204)
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
    success = await broker_connections_service.delete_broker_connection(connection_id, jwt.sub, db_sess)
    if not success:
        raise HTTPException(status_code=404, detail="Broker connection not found")
    await db_sess.commit()


@router.get("/alpaca/oauth", response_model=GetOauthUrlResponse)
async def get_oauth_url(jwt: JWTPayload = Depends(depends_jwt())):
    url = await alpaca_oauth_service.get_oauth_url_v2(jwt.sub, "paper")
    return GetOauthUrlResponse(url=url)


@router.get("/alpaca/oauth/callback")
async def oauth_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    params = [("broker", BrokerType.ALPACA.value)]
    if code is not None:
        await alpaca_oauth_service.handle_oauth_callback(code, state, jwt.sub, db_sess)
    else:
        params.append(("error", error))

    query_params = "&".join(f"{k}={v}" for k, v in params)

    return RedirectResponse(
        f"{SCHEME}://{FRONTEND_SUB_DOMAIN}{FRONTEND_DOMAIN}/brokers/oauth?{query_params}"
    )
