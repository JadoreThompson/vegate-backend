from datetime import datetime

from vegate.core.schema import CustomBaseModel
from .enums import (
    ContractType,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
)


class OrderRequest(CustomBaseModel):
    symbol: str
    quantity: float | None = None
    notional: float | None = None
    order_type: OrderType
    side: OrderSide
    limit_price: float | None = None
    stop_price: float | None = None


class Order(CustomBaseModel):
    """Represents a trading order."""

    id: str
    symbol: str
    quantity: float | None = None
    filled_quantity: float
    notional: float | None = None
    order_type: OrderType
    side: OrderSide
    limit_price: float | None = None
    stop_price: float | None = None
    avg_fill_price: float | None = None
    executed_at: datetime | None = None
    submitted_at: datetime | None = None
    status: OrderStatus = OrderStatus.PENDING
    details: dict[str, object] | None = None


class FuturesOrderRequest(CustomBaseModel):
    symbol: str
    side: OrderSide
    quantity: float | None = None
    notional: float | None = None
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    take_profit: float | None = None
    stop_loss: float | None = None


class FuturesOrder(CustomBaseModel):
    """Represents a futures trading order."""

    id: str
    symbol: str
    side: OrderSide
    quantity: float | None = None
    filled_quantity: float
    notional: float | None = None
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    take_profit: float | None = None
    stop_loss: float | None = None
    avg_fill_price: float | None = None
    reduce_only: bool = False
    executed_at: datetime | None = None
    submitted_at: datetime | None = None
    status: OrderStatus = OrderStatus.PENDING
    details: dict[str, object] | None = None
