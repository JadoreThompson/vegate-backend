from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from models import CustomBaseModel, EquityCurvePoint


class OrderResponse(CustomBaseModel):
    order_id: UUID
    symbol: str
    side: str
    order_type: str
    quantity: float | None
    notional: float | None
    filled_quantity: float
    limit_price: float | None
    stop_price: float | None
    average_fill_price: float | None
    status: str
    submitted_at: datetime
    filled_at: datetime | None
    broker_order_id: str | None


class PerformanceMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    total_trades: int
    equity_curve: list[EquityCurvePoint] = Field(default_factory=list)

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
