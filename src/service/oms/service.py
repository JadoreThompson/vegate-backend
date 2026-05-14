import json
from uuid import UUID, uuid4

from sqlalchemy import select

from enums import BrokerType
from infra.db.model.broker_connections import BrokerConnections
from infra.db.model.strategy import Strategy
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.model.user import User
from infra.db.utils import get_db_session
from service.alpaca.models import AlpacaOAuthPayload
from service.encryption.service import EncryptionService
from service.oms.broker.alpaca import AlpacaBroker
from service.oms.broker.base import Broker
from service.oms.exception import BrokerConnectionDoesNotExistException


class OMSService:

    def __init__(self):
        self._broker_clients: dict[str, Broker] = {}

    async def login(self, deployment_id: UUID, strategy_id: UUID) -> str:
        """
        Connects to the broker for the deployment and returns a unique
        token to identify the connection.

        Args:
            deployment_id (UUID): Id for the deployment
            strategy_id (UUID): Id for the strategy

        Raises:
            BrokerConnectionDoesNotExistException
            ValueError: Generated token already exists

        Returns:
            str: Unique token to identify the broker connection
        """
        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(User.user_id, BrokerConnections)
                .join(StrategyDeployments.broker_connection)
                .join(Strategy, StrategyDeployments.strategy_id == Strategy.strategy_id)
                .join(User, Strategy.user_id == User.user_id)
                .where(StrategyDeployments.deployment_id == deployment_id)
            )
            data = res.scalars().all()
            user_id, broker_conn = data
            if broker_conn is None:
                raise BrokerConnectionDoesNotExistException(deployment_id)

        broker_client = self._build_broker_client(broker_conn, user_id)
        broker_client.connect()
        token = self._generate_token()
        if token in self._broker_clients:
            raise ValueError(f"Token '{token}' already exists")
        self._broker_clients[token] = broker_client
        return token

    def _build_broker_client(
        self, broker_conn: BrokerConnections, user_id: UUID
    ) -> Broker:
        if broker_conn.broker == BrokerType.ALPACA:
            oauth_payload = AlpacaOAuthPayload(
                json.loads(
                    EncryptionService.decrypt(
                        broker_conn.oauth_payload, aad=str(user_id)
                    )
                )
            )
            return AlpacaBroker(
                api_key=broker_conn.api_key,
                secret_key=broker_conn.secret_key,
                oauth_token=oauth_payload.access_token,
            )

        raise ValueError(f"Unsupported broker '{broker_conn.broker}'")
    
    def _generate_token(self) -> str:
        return str(uuid4())

