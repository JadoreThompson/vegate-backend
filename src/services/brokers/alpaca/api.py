import json
import random
import string
from urllib.parse import quote
from uuid import UUID

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    ALPACA_OAUTH_CLIENT_ID,
    ALPACA_OAUTH_REDIRECT_URI,
    ALPACA_OAUTH_SECRET_KEY,
    REDIS_ALPACA_OAUTH_PREFIX,
    REDIS_ALPACA_OAUTH_TTL_SECS,
)
from db_models import BrokerConnections
from engine.enums import BrokerType
from services import EncryptionService
from utils.redis import REDIS_CLIENT
from .models import AlpacaOAuthPayload
from .types import _RedisOAuthPayload, AlpacaTradingEnv
from ..exc import BrokerOAuthError
from ..base import BaseBrokerAPI
from ..mixins import HTTPSessMixin


class AlpacaAPI(HTTPSessMixin, BaseBrokerAPI):
    def __init__(self):
        super().__init__()

    async def get_oauth_url(self, user_id: UUID, env: AlpacaTradingEnv) -> str:
        """Generate OAuth URL for Alpaca authentication."""
        state = "".join(random.choices(string.ascii_uppercase + string.digits, k=24))

        base_url = "https://app.alpaca.markets/oauth/authorize"

        params = (
            ("response_type", "code"),
            ("client_id", ALPACA_OAUTH_CLIENT_ID),
            ("redirect_uri", quote(ALPACA_OAUTH_REDIRECT_URI)),
            ("state", quote(state)),
            ("scope", "trading"),
        )
        query_string = "&".join(f"{key}={value}" for key, value in params)

        payload: _RedisOAuthPayload = {"user_id": str(user_id), "env": env}
        await REDIS_CLIENT.set(
            f"{REDIS_ALPACA_OAUTH_PREFIX}{state}",
            json.dumps(payload),
            ex=REDIS_ALPACA_OAUTH_TTL_SECS,
        )

        return f"{base_url}?{query_string}"

    async def handle_oauth_callback(
        self, code: str, state: str, user_id: UUID, db_sess: AsyncSession
    ):
        """Handle OAuth callback from Alpaca and store credentials."""
        data = await REDIS_CLIENT.get(f"{REDIS_ALPACA_OAUTH_PREFIX}{state}")
        if not data:
            raise ValueError("OAuth not requested or expired")

        payload: _RedisOAuthPayload = json.loads(data)
        if payload["user_id"] != str(user_id):
            raise ValueError("Invalid state")

        body = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": ALPACA_OAUTH_CLIENT_ID,
            "client_secret": ALPACA_OAUTH_SECRET_KEY,
            "redirect_uri": ALPACA_OAUTH_REDIRECT_URI,
        }

        rsp = await self.http_session.post(
            "https://api.alpaca.markets/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )

        data = await rsp.json()
        if not 200 <= rsp.status <= 300:
            raise BrokerOAuthError(data["message"])

        data["env"] = payload["env"]

        oauth_payload = AlpacaOAuthPayload(**data)
        encrypted_payload = EncryptionService.encrypt(
            oauth_payload.model_dump(mode="json"), aad=str(user_id)
        )

        # Fetch account
        base_url = self._get_base_url(oauth_payload.env)
        endpoint = "/account"
        headers = {"Authorization": f"Bearer {oauth_payload.access_token}"}

        rsp = await self._http_sess.get(f"{base_url}{endpoint}", headers=headers)
        rsp.raise_for_status()
        data = await rsp.json()
        account_id = data["account_number"]

        # Persisting
        existing_conn = await db_sess.scalar(
            select(BrokerConnections).where(
                BrokerConnections.broker == BrokerType.ALPACA,
                BrokerConnections.broker_account_id == account_id,
            )
        )
        if existing_conn is None:
            await db_sess.execute(
                insert(BrokerConnections).values(
                    user_id=user_id,
                    broker=BrokerType.ALPACA,
                    oauth_payload=encrypted_payload,
                    broker_account_id=account_id,
                )
            )
        await db_sess.commit()

    @staticmethod
    def _get_base_url(env: AlpacaTradingEnv):
        if env == "live":
            return "https://api.alpaca.markets/v2"
        return "https://paper-api.alpaca.markets/v2"
