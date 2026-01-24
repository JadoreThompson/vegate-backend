from pydantic import BaseModel


class AlpacaConnectRequest(BaseModel):
    api_key: str
    secret_key: str
