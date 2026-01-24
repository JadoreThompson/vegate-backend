from uuid import UUID

from pydantic import BaseModel

from enums import BrokerType
from models import CustomBaseModel


class GetOauthUrlResponse(BaseModel):
    url: str


class BrokerConnectionResponse(CustomBaseModel):
    """Response model for broker connection details."""

    connection_id: UUID
    broker: BrokerType
    broker_account_id: str
