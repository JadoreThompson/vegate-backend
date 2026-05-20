from uuid import UUID

from pydantic import BaseModel

from models import CustomBaseModel, Order
from service.oms.broker_client.model import OrderRequest


class CreateSessionRequest(CustomBaseModel):
    deployment_id: UUID


class CreateSessionResponse(BaseModel):
    token: str


class PlaceOrderRequest(CustomBaseModel):
    order: OrderRequest
    candle_ts: int


class ModifyOrderRequest(BaseModel):
    limit_price: float | None = None
    stop_price: float | None = None


class OrderResponse(Order):
    pass


class SuccessResponse(BaseModel):
    success: bool


class BalanceResponse(BaseModel):
    balance: float


class EquityResponse(BaseModel):
    equity: float

class PositionResponse(BaseModel):
    balance: float
