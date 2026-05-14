from uuid import UUID

from pydantic import BaseModel

from enums import OrderType, OrderSide
from models import CustomBaseModel, Order


class CreateSessionRequest(CustomBaseModel):
    deployment_id: UUID


class CreateSessionResponse(BaseModel):
    token: str


class PlaceOrderRequest(CustomBaseModel):
    symbol: str
    quantity: float | None = None
    notional: float | None = None
    order_type: OrderType
    side: OrderSide
    limit_price: float | None = None
    stop_price: float | None = None


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
