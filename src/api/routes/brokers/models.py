from pydantic import BaseModel


class GetOauthUrlResponse(BaseModel):
    url: str