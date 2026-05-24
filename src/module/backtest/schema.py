from datetime import datetime, date
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from module.broker.enums import OrderStatus
from .enums import BacktestStatus
from core.schema import CustomBaseModel


class CreateBacktestRequest(BaseModel):
    strategy_id: UUID
    starting_balance: int = Field(gt=0, le=100_000)
    start_date: datetime
    end_date: datetime


class CreateBacktestResponse(BaseModel):
    id: UUID


class EquityCurvePoint(BaseModel):
    """Represents a point in the equity curve."""

    timestamp: datetime
    balance: float
    equity: float


class BacktestMetricsSchema(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    total_orders: int
    equity_curve: list[EquityCurvePoint]


class BacktestResponse(CustomBaseModel):
    id: UUID
    strategy_id: UUID
    starting_balance: float
    start_date: date
    end_date: date
    status: BacktestStatus
    created_at: datetime
    metrics: BacktestMetricsSchema | None = None


class BacktestOrderResponse(BaseModel):
    id: UUID
    backtest_id: UUID
    symbol: str
    side: str
    order_type: str
    quantity: float | None = None
    notional: float | None = None
    filled_quantity: float = 0
    limit_price: float | None = None
    stop_price: float | None = None
    avg_fill_price: float | None = None
    status: OrderStatus
    submitted_at: datetime
    filled_at: datetime | None = None
    details: dict[str, Any] | None = None
