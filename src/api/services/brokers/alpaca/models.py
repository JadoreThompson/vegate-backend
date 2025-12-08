from uuid import UUID

from pydantic import BaseModel

from .types import AlpacaTradingEnv


class _AlpacaOAuthPayload(BaseModel):
    access_token: str
    token_type: str
    scope: str
    env: AlpacaTradingEnv
