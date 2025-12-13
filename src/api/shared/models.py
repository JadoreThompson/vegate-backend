from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from core.models import CustomBaseModel
from engine.backtesting.types import EquityCurve


T = TypeVar("T")


class PaginationMeta(BaseModel):
    page: int
    size: int
    has_next: bool


class PaginatedResponse(PaginationMeta, Generic[T]):
    data: list[T]


class OrderResponse(CustomBaseModel):
    order_id: UUID
    symbol: str
    side: str
    order_type: str
    quantity: float
    filled_quantity: float
    limit_price: float | None
    stop_price: float | None
    average_fill_price: float | None
    status: str
    time_in_force: str
    submitted_at: datetime
    filled_at: datetime | None
    client_order_id: str | None
    broker_order_id: str | None


# class EquityCurve(BaseModel):
#     equity_curve: EquityCurveT


class PerformanceMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    total_trades: int
    equity_curve: EquityCurve = Field(default_factory=list)

    @field_validator(
        "realised_pnl",
        "total_return_pct",
        "unrealised_pnl",
        "sharpe_ratio",
        "max_drawdown",
        mode="after",
    )
    def round_values(cls, v):
        return round(v, 2)
