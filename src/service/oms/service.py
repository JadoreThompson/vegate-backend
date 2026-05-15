import json
import logging
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from enums import BrokerType, OrderStatus
from infra.db.model.broker_connections import BrokerConnections
from infra.db.model.strategy import Strategy
from infra.db.model.strategy_deployment_orders import StrategyDeploymentOrders
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.model.user import User
from infra.db.utils import get_db_session
from service.alpaca.models import AlpacaOAuthPayload
from service.encryption.service import EncryptionService
from service.oms.broker_client.alpaca import AlpacaBrokerClient
from service.oms.broker_client.base import BrokerClient
from service.oms.broker_client.exception import BrokerClientException
from service.oms.exception import (
    BrokerConnectionDoesNotExistException,
    DuplicateOrderException,
    OrderNotFoundException,
)
from service.oms.broker_client.model import Order
from service.oms.server.model import PlaceOrderRequest


class Session:

    def __init__(self, deployment_id: UUID, broker_client: BrokerClient):
        self.deployment_id = deployment_id
        self.broker_client = broker_client


class OMSService:

    def __init__(self):
        self._broker_clients: dict[str, Session] = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    async def create_session(self, deployment_id: UUID) -> str:
        """
        Connects to the broker for the deployment and returns a unique token.
        """
        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(User.user_id, BrokerConnections)
                .join(StrategyDeployments.broker_connection)
                .join(Strategy, StrategyDeployments.strategy_id == Strategy.strategy_id)
                .join(User, Strategy.user_id == User.user_id)
                .where(StrategyDeployments.deployment_id == deployment_id)
            )

            row = res.first()
            if not row:
                raise BrokerConnectionDoesNotExistException(deployment_id)

            user_id, broker_conn = row

        broker_client = self._build_broker_client(broker_conn, user_id)
        broker_client.connect()

        session = Session(deployment_id=deployment_id, broker_client=broker_client)

        token = self._generate_token()
        if token in self._broker_clients:
            raise ValueError(f"Token '{token}' already exists")

        self._broker_clients[token] = session
        return token

    def close_session(self, token: str) -> None:
        session = self._broker_clients.get(token)

        if not session:
            raise ValueError(f"Invalid or expired token '{token}'")

        try:
            session.broker_client.disconnect()
        except Exception as e:
            raise RuntimeError(f"Failed to disconnect broker client: {e}")

        self._broker_clients.pop(token, None)

    def _get_broker(self, token: str) -> BrokerClient:
        session = self._broker_clients.get(token)
        if not session:
            raise ValueError(f"Invalid or expired token '{token}'")
        return session.broker_client

    def _get_session(self, token: str) -> Session:
        session = self._broker_clients.get(token)
        if not session:
            raise ValueError(f"Invalid or expired token '{token}'")
        return session

    def get_balance(self, token: str) -> float:
        return self._get_broker(token).get_balance()

    def get_equity(self, token: str) -> float:
        return self._get_broker(token).get_equity()

    async def place_order(self, token: str, request: PlaceOrderRequest) -> Order:
        session = self._get_session(token)

        async with get_db_session() as db_sess:
            key = await self._generate_order_key(
                session.deployment_id, request.candle_ts, db_sess
            )
            await self._ensure_unique_key(key, session.deployment_id, db_sess)

        stmt = insert(StrategyDeploymentOrders).returning(StrategyDeploymentOrders.id)
        try:
            order = session.broker_client.place_order(request.order)
            stmt = stmt.values(
                deployment_id=session.deployment_id,
                symbol=order.symbol,
                quantity=order.quantity,
                notional=order.notional,
                side=order.side,
                limit_price=order.limit_price,
                stop_price=order.stop_price,
                candle_ts=request.candle_ts,
                status=order.status,
                key=key,
                request_payload=json.dumps(request.model_dump(mode="json")),
                broker_order_id=order.id,
            )
        except BrokerClientException:
            stmt = stmt.values(
                deployment_id=session.deployment_id,
                symbol=request.order.symbol,
                quantity=request.order.quantity,
                notional=request.order.notional,
                side=request.order.side,
                limit_price=request.order.limit_price,
                stop_price=request.order.stop_price,
                candle_ts=request.candle_ts,
                status=OrderStatus.REJECTED,
                key=key,
                request_payload=json.dumps(request.model_dump(mode="json")),
            )

        async with get_db_session() as db_sess:
            res = await db_sess.execute(stmt)
            order_id = res.scalar()
            await db_sess.commit()

        order.id = order_id
        return order

    async def modify_order(
        self,
        token: str,
        order_id: UUID,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        broker_order_id = await self._get_broker_order_id(order_id)

        order = self._get_broker(token).modify_order(
            broker_order_id, limit_price, stop_price
        )

        async with get_db_session() as db_sess:
            await db_sess.execute(
                update(StrategyDeploymentOrders)
                .where(StrategyDeploymentOrders.id == order_id)
                .values(
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    status=order.status,
                    broker_order_id=order.id,
                )
            )
            await db_sess.commit()

        order.id = order_id
        return order

    async def cancel_order(self, token: str, order_id: UUID) -> bool:
        broker_order_id = await self._get_broker_order_id(order_id)

        success = self._get_broker(token).cancel_order(broker_order_id)

        async with get_db_session() as db_sess:
            await db_sess.execute(
                update(StrategyDeploymentOrders)
                .where(StrategyDeploymentOrders.id == order_id)
                .values(status=OrderStatus.CANCELLED if success else OrderStatus.PLACED)
            )
            await db_sess.commit()

        return success

    async def get_order(self, token: str, order_id: UUID) -> Order:
        broker_order_id = await self._get_broker_order_id(order_id)

        order = self._get_broker(token).get_order(broker_order_id)
        if order is None:
            raise OrderNotFoundException(order_id)

        order.id = order_id
        return order

    async def cancel_all_orders(self, token: str) -> bool:
        session = self._get_session(token)

        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(StrategyDeploymentOrders.id).where(
                    StrategyDeploymentOrders.deployment_id == session.deployment_id,
                    StrategyDeploymentOrders.status == OrderStatus.PLACED,
                )
            )
            order_ids = res.scalars().all()

        if not order_ids:
            return True

        success = self._get_broker(token).cancel_all_orders()
        if success:
            async with get_db_session() as db_sess:
                await db_sess.execute(
                    update(StrategyDeploymentOrders)
                    .where(StrategyDeploymentOrders.id.in_(order_ids))
                    .values(status=OrderStatus.CANCELLED)
                )
                await db_sess.commit()

        return success

    async def get_orders(self, token: str, deployment_id: UUID) -> list[Order]:
        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(
                    StrategyDeploymentOrders.id,
                    StrategyDeploymentOrders.broker_order_id,
                ).where(StrategyDeploymentOrders.deployment_id == deployment_id)
            )
            rows = res.all()

        broker_id_to_internal: dict[str, UUID] = {
            str(broker_order_id): internal_id for internal_id, broker_order_id in rows
        }

        orders = self._get_broker(token).get_orders()

        result = []
        for order in orders:
            internal_id = broker_id_to_internal.get(order.id)
            if internal_id is not None:
                order.id = internal_id
                result.append(order)

        return result

    def _build_broker_client(
        self, broker_conn: BrokerConnections, user_id: UUID
    ) -> BrokerClient:
        if broker_conn.broker == BrokerType.ALPACA:
            if broker_conn.oauth_payload is not None:
                oauth_payload = AlpacaOAuthPayload.model_validate(
                    json.loads(
                        EncryptionService.decrypt(
                            broker_conn.oauth_payload, expected_aad=str(user_id)
                        )
                    )
                )
                oauth_token = oauth_payload.access_token
            else:
                oauth_token = None

            return AlpacaBrokerClient(
                api_key=broker_conn.api_key,
                secret_key=broker_conn.secret_key,
                oauth_token=oauth_token,
            )

        raise ValueError(f"Unsupported broker '{broker_conn.broker}'")

    def _generate_token(self) -> str:
        return str(uuid4())

    async def _generate_order_key(
        self, deployment_id: UUID, candle_ts: int, db_sess: AsyncSession
    ) -> str:
        res = await db_sess.execute(
            select(func.count(StrategyDeploymentOrders.id)).where(
                StrategyDeploymentOrders.deployment_id == deployment_id
            )
        )
        count = res.scalar()
        return f"{candle_ts}-{count}"

    async def _ensure_unique_key(
        self, key: str, deployment_id: UUID, db_sess: AsyncSession
    ) -> None:
        res = await db_sess.execute(
            select(StrategyDeploymentOrders).where(
                StrategyDeploymentOrders.deployment_id == deployment_id,
                StrategyDeploymentOrders.key == key,
            )
        )
        if res.first():
            raise DuplicateOrderException()

    async def _get_broker_order_id(self, order_id: UUID) -> str:
        """
        Resolves an internal DB order UUID to the broker-native order ID.

        Raises:
            OrderNotFoundException: if no order row exists for the given order_id
        """
        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(StrategyDeploymentOrders.broker_order_id).where(
                    StrategyDeploymentOrders.id == order_id
                )
            )
            broker_order_id = res.scalar()

        if broker_order_id is None:
            raise OrderNotFoundException(order_id)

        return broker_order_id
