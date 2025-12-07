import json
import random
import string
from urllib.parse import quote
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.services import EncryptionService
from config import (
    ALPACA_OAUTH_CLIENT_ID,
    ALPACA_OAUTH_REDIRECT_URI,
    ALPACA_OAUTH_SECRET_KEY,
)
from db_models import BrokerConnections
from engine.enums import BrokerPlatformType
from .base import BaseBrokerAPI
from .mixins import HTTPSessMixin


class AlpacaAPI(HTTPSessMixin, BaseBrokerAPI):
    def __init__(self):
        """Initialize AlpacaAPI with HTTP session."""
        super().__init__()

    def get_oauth_url(self) -> str:
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

        return f"{base_url}?{query_string}"

    async def handle_oauth_callback(
        self, code: str, user_id: UUID, db_sess: AsyncSession
    ):
        """Handle OAuth callback from Alpaca and store credentials."""
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
