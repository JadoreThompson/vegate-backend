from pydantic import BaseModel
from typing import Literal, TypedDict

AlpacaTradingEnv = Literal["live", "paper"]


class AlpacaOAuthPayload(BaseModel):
    access_token: str
    token_type: str
    scope: str
    env: AlpacaTradingEnv


class RedisOAuthPayload(TypedDict):
    user_id: str
    env: AlpacaTradingEnv
