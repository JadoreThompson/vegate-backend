from typing import Any
from datetime import datetime

from pydantic import BaseModel, Field

from core.models import CustomBaseModel
from engine.enums import OrderType, OrderSide, OrderStatus, TimeInForce


class OrderRequest(BaseModel):
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float | None = Field(None, gt=0)
    notional: float | None = None
    limit_price: float | None = Field(None, gt=0)
    stop_price: float | None = Field(None, gt=0)
    time_in_force: TimeInForce
    client_order_id: str | None = None

    def model_post_init(self, context):
        if self.quantity is None and self.notional is None:
            raise ValueError("Either quantity or notional value must be provided")
        if self.order_type == OrderType.MARKET and (
            self.limit_price is not None or self.stop_price is not None
        ):
            raise ValueError("Market order cannot define limit_price or stop_price")
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("Limit price must be defined for limit order")
        if self.order_type == OrderType.STOP and self.stop_price is None:
            raise ValueError("Stop price must be defined for stop order")
        return self


class OrderResponse(CustomBaseModel):
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    filled_quantity: float
    limit_price: float | None
    stop_price: float | None
    status: OrderStatus
    created_at: datetime
    filled_at: datetime | None = None
    avg_fill_price: float | None = None
    time_in_force: TimeInForce
    broker_metadata: dict[str, Any] = Field(default_factory=dict)


class Account(BaseModel):
    account_id: str
    equity: float
    cash: float
