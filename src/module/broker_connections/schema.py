from uuid import UUID

from pydantic import BaseModel

from vegate.oms.enums import BrokerType
from core.schema import CustomBaseModel


class GetOauthUrlResponse(BaseModel):
    url: str


class CreateBrokerConnectionRequest(CustomBaseModel):
    broker: BrokerType
    api_key: str
    secret_key: str


class BrokerConnectionResponse(CustomBaseModel):
    id: UUID
    broker: BrokerType
    account_id: str
    account_number: str
