from dataclasses import dataclass
from datetime import datetime

from vegate.oms.schema import Order


@dataclass
class EquityCurvePoint:
    """Represents a point in the equity curve."""

    timestamp: datetime
    balance: float
    equity: float


@dataclass
class BacktestMetrics:
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    total_orders: int
    equity_curve: list[EquityCurvePoint]
    orders: list[Order]
