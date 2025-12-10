from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from core.models import CustomBaseModel
from engine.enums import BrokerType


class GetOauthUrlResponse(BaseModel):
    url: str


class BrokerConnectionResponse(CustomBaseModel):
    """Response model for broker connection details."""

    connection_id: UUID
    broker: BrokerType
    broker_account_id: str
    created_at: datetime = None
