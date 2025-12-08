from typing import Literal, TypedDict


AlpacaTradingEnv = Literal["live", "paper"]


class _RedisOAuthPayload(TypedDict):
    user_id: str
    env: AlpacaTradingEnv
