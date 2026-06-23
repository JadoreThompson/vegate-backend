from dataclasses import dataclass

from vegate.oms.schema import Order


@dataclass
class EquityCurvePoint:
    timestamp: int
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
