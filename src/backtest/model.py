from datetime import datetime

from pydantic import BaseModel, field_validator

from models import Order


class EquityCurvePoint(BaseModel):
    """Represents a point in the equity curve."""

    timestamp: datetime
    balance: float
    equity: float


class BacktestMetrics(BaseModel):
    """Represents backtest performance metrics."""

    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    equity_curve: list[EquityCurvePoint]
    orders: list[Order]
    total_orders: int

    @field_validator(
        "realised_pnl",
        "unrealised_pnl",
        "total_return_pct",
        "profit_factor",
        mode="after",
    )
    def round_values(cls, value):
        return round(value, 2)
