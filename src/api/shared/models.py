from datetime import datetime
from decimal import Decimal
from typing import Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel

from core.models import CustomBaseModel
from engine.backtesting.types import EquityCurveT


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
    quantity: Decimal
    filled_quantity: Decimal
    limit_price: Decimal | None
    stop_price: Decimal | None
    average_fill_price: Decimal | None
    status: str
    time_in_force: str
    submitted_at: datetime
    filled_at: datetime | None
    client_order_id: str | None
    broker_order_id: str | None


class PerformanceMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    total_trades: int
    equity_curve: EquityCurveT
