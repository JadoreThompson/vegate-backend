import json
import random
import string
from urllib.parse import quote, urlencode
from uuid import UUID

import aiohttp
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    ALPACA_OAUTH_CLIENT_ID,
    ALPACA_OAUTH_REDIRECT_URI,
    ALPACA_OAUTH_SECRET_KEY,
    REDIS_ALPACA_OAUTH_PREFIX,
    REDIS_ALPACA_OAUTH_TTL_SECS,
)
from core.redis import REDIS_CLIENT
from vegate.oms.enums import BrokerType
from module.broker_connections.model import BrokerConnections
from .exception import AlpacaOauthException
from .schema import AlpacaOAuthPayload, RedisOAuthPayload, AlpacaTradingEnv
from ..encryption import EncryptionService


class AlpacaOauthService:

    def __init__(self, redis_client: AsyncRedis = REDIS_CLIENT):
        self._redis_client = redis_client
        self._http_sess: aiohttp.ClientSession | None = None

    def get_http_session(self):
        if self._http_sess is None:
            self._http_sess = aiohttp.ClientSession()
        return self._http_sess

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
            ("scope", "data"),
        )
        query_string = "&".join(f"{key}={value}" for key, value in params)

        payload: RedisOAuthPayload = {"user_id": str(user_id), "env": env}
        await self._redis_client.set(
            f"{REDIS_ALPACA_OAUTH_PREFIX}{state}",
            json.dumps(payload),
            ex=REDIS_ALPACA_OAUTH_TTL_SECS,
        )

        return f"{base_url}?{query_string}"

    async def get_oauth_url_v2(self, user_id: UUID, env: AlpacaTradingEnv) -> str:
        """Generate OAuth URL for Alpaca authentication."""
        state = "".join(random.choices(string.ascii_uppercase + string.digits, k=24))

        base_url = "https://app.alpaca.markets/oauth/authorize"

        scopes = ["trading", "data"]

        params = {
            "response_type": "code",
            "client_id": ALPACA_OAUTH_CLIENT_ID,
            "redirect_uri": ALPACA_OAUTH_REDIRECT_URI,
            "state": state,
            "scope": " ".join(scopes),
        }

        query_string = urlencode(params)

        payload: RedisOAuthPayload = {
            "user_id": str(user_id),
            "env": env,
        }

        await self._redis_client.set(
            f"{REDIS_ALPACA_OAUTH_PREFIX}{state}",
            json.dumps(payload),
            ex=REDIS_ALPACA_OAUTH_TTL_SECS,
        )

        return f"{base_url}?{query_string}"

    async def handle_oauth_callback(
        self, code: str, state: str, user_id: UUID, db_sess: AsyncSession
    ):
        """Handle OAuth callback from Alpaca and store credentials."""
        data = await self._redis_client.get(f"{REDIS_ALPACA_OAUTH_PREFIX}{state}")
        if not data:
            raise ValueError("OAuth not requested or expired")

        payload: RedisOAuthPayload = json.loads(data)
        if payload["user_id"] != str(user_id):
            raise ValueError("Invalid state")

        body = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": ALPACA_OAUTH_CLIENT_ID,
            "client_secret": ALPACA_OAUTH_SECRET_KEY,
            "redirect_uri": ALPACA_OAUTH_REDIRECT_URI,
        }

        rsp = await self.get_http_session().post(
            "https://api.alpaca.markets/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )

        data = await rsp.json()
        if not 200 <= rsp.status <= 300:
            raise AlpacaOauthException(data["message"])

        data["env"] = payload["env"]

        oauth_payload = AlpacaOAuthPayload(**data)
        encrypted_payload = EncryptionService.encrypt(
            oauth_payload.model_dump(mode="json"), aad=str(user_id)
        )

        # Fetch account
        base_url = self._get_base_url(oauth_payload.env)
        endpoint = "/account"
        headers = {"Authorization": f"Bearer {oauth_payload.access_token}"}

        rsp = await self.get_http_session().get(
            f"{base_url}{endpoint}", headers=headers
        )
        rsp.raise_for_status()
        data = await rsp.json()
        account_id = data["id"]
        account_number = data["account_number"]

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
                    broker_account_number=account_number,
                )
            )
        await db_sess.commit()

    @staticmethod
    def _get_base_url(env: AlpacaTradingEnv):
        if env == "live":
            return "https://api.alpaca.markets/v2"
        return "https://paper-api.alpaca.markets/v2"
