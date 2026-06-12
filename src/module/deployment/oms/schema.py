from uuid import UUID

from pydantic import BaseModel

from core.schema import CustomBaseModel
from vegate.oms.schema import Order, OrderRequest


class CreateSessionRequest(CustomBaseModel):
    deployment_id: UUID


class CreateSessionResponse(BaseModel):
    token: str


class PlaceOrderRequest(CustomBaseModel):
    order: OrderRequest


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
