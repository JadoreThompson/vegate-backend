import json
import random
import string
from urllib.parse import quote, urlencode
from uuid import UUID

import aiohttp
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    ALPACA_OAUTH_CLIENT_ID,
    ALPACA_OAUTH_REDIRECT_URI,
    ALPACA_OAUTH_SECRET_KEY,
    REDIS_ALPACA_OAUTH_PREFIX,
    REDIS_ALPACA_OAUTH_TTL_SECS,
)
from enums import BrokerType
from infra.db.models import BrokerConnections
from infra.redis import REDIS_CLIENT
from services.encryption import EncryptionService
from .exc import AlpacaOauthError
from .models import AlpacaOAuthPayload
from .types import _RedisOAuthPayload, AlpacaTradingEnv


class AlpacaService:
    def __init__(self):
        self._http_sess = aiohttp.ClientSession()

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

        payload: _RedisOAuthPayload = {"user_id": str(user_id), "env": env}
        await REDIS_CLIENT.set(
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

        payload: _RedisOAuthPayload = {
            "user_id": str(user_id),
            "env": env,
        }

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

        rsp = await self._http_sess.post(
            "https://api.alpaca.markets/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )

        data = await rsp.json()
        if not 200 <= rsp.status <= 300:
            raise AlpacaOauthError(data["message"])

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

    async def get_account(self, api_key: str, secret_key: str) -> dict:
        """Fetch Alpaca account details using API keys."""
        base_url = self._get_base_url("paper")
        endpoint = "/account"
        headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
        }

        rsp = await self._http_sess.get(f"{base_url}{endpoint}", headers=headers)
        rsp.raise_for_status()
        data = await rsp.json()
        return data
    

    @staticmethod
    def _get_base_url(env: AlpacaTradingEnv):
        if env == "live":
            return "https://api.alpaca.markets/v2"
        return "https://paper-api.alpaca.markets/v2"
