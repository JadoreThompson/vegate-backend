from dataclasses import dataclass
from uuid import UUID

import aiohttp
from sqlalchemy import and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.broker_connections.exception import (
    BrokerAccountFetchException,
    BrokerConnectionNotFoundException,
    UnsupportedBrokerException,
)
from api.routes.broker_connections.model import (
    BrokerConnectionResponse,
    CreateBrokerConnectionRequest,
)
from enums import BrokerType
from infra.db.model.broker_connections import BrokerConnections


@dataclass(slots=True, frozen=True)
class _BrokerAccount:
    id: str
    number: str


class BrokerConnectionsService:

    def __init__(self):
        self._http_sess: aiohttp.ClientSession | None = None

    def get_http_session(self):
        if self._http_sess is None:
            self._http_sess = aiohttp.ClientSession()
        return self._http_sess

    async def create_broker_connection(
        self,
        request: CreateBrokerConnectionRequest,
        user_id: UUID,
        db_sess: AsyncSession,
    ) -> BrokerConnections:
        if request.broker == BrokerType.ALPACA:
            account = await self._fetch_alpaca_account_id(
                request.api_key, request.secret_key
            )
        else:
            raise UnsupportedBrokerException(request.broker)

        broker_connection = BrokerConnections(
            user_id=user_id,
            broker=request.broker,
            api_key=request.api_key,
            secret_key=request.secret_key,
            broker_account_id=account.id,
            broker_account_number=account.number,
        )

        db_sess.add(broker_connection)
        await db_sess.flush()
        await db_sess.refresh(broker_connection)

        return broker_connection

    async def _fetch_alpaca_account_id(
        self, api_key: str, secret_key: str
    ) -> _BrokerAccount:
        url = "https://paper-api.alpaca.markets/v2/account"
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}
        rsp = await self.get_http_session().get(url, headers=headers)
        if not rsp.ok:
            raise BrokerAccountFetchException(
                "Failed to fetch account. Ensure the provided keys are correct"
            )

        body = await rsp.json()
        return _BrokerAccount(id=body["id"], number=body["account_number"])

    async def get_broker_connections(
        self, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ) -> PaginatedResponse[BrokerConnectionResponse]:
        res = await db_sess.execute(
            select(BrokerConnections)
            .where(BrokerConnections.user_id == user_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        conns = [
            BrokerConnectionResponse(
                id=conn.connection_id,
                broker=conn.broker,
                account_id=conn.broker_account_id,
                account_number=conn.broker_account_number,
            )
            for conn in res.scalars().all()
        ]

        return PaginatedResponse[BrokerConnectionResponse](
            page=page,
            size=min(limit, len(conns)),
            has_next=len(conns) > limit,
            data=conns[:limit],
        )

    async def get_broker_connection(
        self, id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> BrokerConnections:
        conn = await db_sess.scalar(
            select(BrokerConnections).where(
                and_(
                    BrokerConnections.connection_id == id,
                    BrokerConnections.user_id == user_id,
                )
            )
        )

        if conn is None:
            raise BrokerConnectionNotFoundException()
        
        return conn

    async def delete_broker_connection(
        self, id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> bool:
        result = await db_sess.execute(
            delete(BrokerConnections).where(
                BrokerConnections.connection_id == id,
                BrokerConnections.user_id == user_id,
            )
        )

        return result.rowcount > 0

    async def close(self):
        if not self._http_sess.closed:
            await self._http_sess.close()
