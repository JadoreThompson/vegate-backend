import json
import random
import string
from typing import Literal
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.services.mixins import HTTPSessMixin
from api.services import EncryptionService
from config import (
    ALPACA_OAUTH_CLIENT_ID,
    ALPACA_OAUTH_REDIRECT_URI_DEMO,
    ALPACA_OAUTH_SECRET_KEY,
)
from db_models import BrokerConnections
from engine.enums import BrokerPlatformType
from .base import BaseBrokerAPI


AlpacaTradingEnvT = Literal["demo", "live"]


class AlpacaAPI(BaseBrokerAPI, HTTPSessMixin):
    def get_oauth_url(self, env: AlpacaTradingEnvT) -> str:
        state = "".join(random.choices(string.ascii_uppercase + string.digits, k=24))
        scope = "trading"
        env = "live" if env == "live" else "paper"

        base_url = "https://app.alpaca.markets/oauth/authorize"
        params = (
            f"response_type=code&"
            f"client_id={ALPACA_OAUTH_CLIENT_ID}&"
            f"redirect_uri={ALPACA_OAUTH_REDIRECT_URI_DEMO}&"
            f"state={state}&"
            f"scope={scope}&"
            f"env={env}"
        )

        return f"{base_url}?{params}"

    async def handle_oauth_callback(
        self, user_id: UUID, code: str, env: AlpacaTradingEnvT, db_sess: AsyncSession
    ):
        if env == "demo":
            redirect_uri = ALPACA_OAUTH_REDIRECT_URI_DEMO
        else:
            raise NotImplementedError(
                f"Oauth for trading environment '{env}' not implemented"
            )

        body = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": ALPACA_OAUTH_CLIENT_ID,
            "client_secret": ALPACA_OAUTH_SECRET_KEY,
            "redirect_uri": redirect_uri,
        }

        rsp = await self._http_sess.post(
            "https://api.alpaca.markets/oauth/token", json=body
        )
        rsp.raise_for_status()

        data = await rsp.json()
        encrypted_payload = EncryptionService.encrypt(json.dumps(data))

        await db_sess.execute(
            insert(BrokerConnections).values(
                user_id=user_id,
                broker=BrokerPlatformType.ALPACA,
                oauth_payload=encrypted_payload,
            )
        )
        await db_sess.commit()
