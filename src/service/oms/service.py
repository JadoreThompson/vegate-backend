import json
from uuid import UUID, uuid4

from sqlalchemy import select

from enums import BrokerType
from infra.db.model.broker_connections import BrokerConnections
from infra.db.model.strategy import Strategy
from infra.db.model.strategy_deployment_orders import StrategyDeploymentOrders
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.model.user import User
from infra.db.utils import get_db_session
from models import OrderRequest, Order
from service.alpaca.models import AlpacaOAuthPayload
from service.encryption.service import EncryptionService
from service.oms.broker_client.alpaca import AlpacaBrokerClient
from service.oms.broker_client.base import BrokerClient
from service.oms.exception import BrokerConnectionDoesNotExistException
from service.oms.server.model import PlaceOrderRequest


class Session:

    def __init__(self, depoyment_id: UUID, broker_client: BrokerClient):
        self.deployment_id = depoyment_id
        self.broker_client = broker_client


# TODO: Implement async API
class OMSService:

    def __init__(self):
        self._broker_clients: dict[str, Session] = {}

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

    def _get_session(self, token: str) -> BrokerClient:
        session = self._broker_clients.get(token)
        if not session:
            raise ValueError(f"Invalid or expired token '{token}'")
        return session

    def get_balance(self, token: str) -> float:
        return self._get_broker(token).get_balance()

    def get_equity(self, token: str) -> float:
        return self._get_broker(token).get_equity()

    async def place_order(self, token: str, request: PlaceOrderRequest) -> Order:
        # return self._get_broker(token).place_order(request)
        session = self._get_session(token)
        async with get_db_session() as db_sess:
            await db_sess.execute(select(StrategyDeploymentOrders).where())

    def modify_order(
        self,
        token: str,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        return self._get_broker(token).modify_order(order_id, limit_price, stop_price)

    def cancel_order(self, token: str, order_id: str) -> bool:
        return self._get_broker(token).cancel_order(order_id)

    def cancel_all_orders(self, token: str) -> bool:
        return self._get_broker(token).cancel_all_orders()

    def get_order(self, token: str, order_id: str) -> Order | None:
        return self._get_broker(token).get_order(order_id)

    def get_orders(self, token: str) -> list[Order]:
        return self._get_broker(token).get_orders()

    def _build_broker_client(
        self, broker_conn: BrokerConnections, user_id: UUID
    ) -> BrokerClient:
        if broker_conn.broker == BrokerType.ALPACA:
            oauth_payload = AlpacaOAuthPayload(
                json.loads(
                    EncryptionService.decrypt(
                        broker_conn.oauth_payload, aad=str(user_id)
                    )
                )
            )

            return AlpacaBrokerClient(
                api_key=broker_conn.api_key,
                secret_key=broker_conn.secret_key,
                oauth_token=oauth_payload.access_token,
            )

        raise ValueError(f"Unsupported broker '{broker_conn.broker}'")

    def _generate_token(self) -> str:
        return str(uuid4())
