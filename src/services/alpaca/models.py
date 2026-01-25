from pydantic import BaseModel

from .types import AlpacaTradingEnv


class AlpacaOAuthPayload(BaseModel):
    access_token: str
    token_type: str
    scope: str
    env: AlpacaTradingEnv
