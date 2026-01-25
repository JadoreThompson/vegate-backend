from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, field_validator

from enums import OrderSide, OrderType, OrderStatus, BrokerType, Timeframe


class CustomBaseModel(BaseModel):
    model_config = {
        "json_encoders": {
            UUID: str,
            datetime: lambda dt: dt.isoformat(),
            Enum: lambda e: e.value,
        }
    }


class OrderRequest(BaseModel):
    """Represents an order request."""

    symbol: str
    quantity: float | None = None
    notional: float | None = None
    order_type: OrderType
    side: OrderSide
    limit_price: float | None = None
    stop_price: float | None = None


class Order(BaseModel):
    """Represents a trading order."""

    order_id: str
    symbol: str
    quantity: float | None = None
    executed_quantity: float
    notional: float | None = None
    order_type: OrderType
    side: OrderSide
    limit_price: float | None = None
    stop_price: float | None = None
    filled_avg_price: float | None = None
    executed_at: datetime | None = None
    submitted_at: datetime | None = None
    status: OrderStatus = OrderStatus.PENDING
    details: dict[str, object] | None = None


class OHLC(BaseModel):
    """Represents an OHLC candle."""

    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    timeframe: Timeframe
    symbol: str


class Tick(BaseModel):
    """Represents a single tick."""

    symbol: str
    timestamp: datetime
    price: float


class BacktestConfig(CustomBaseModel):
    """Configuration for backtesting."""

    timeframe: Timeframe
    starting_balance: float
    symbol: str
    start_date: datetime
    end_date: datetime
    broker: BrokerType


class EquityCurvePoint(BaseModel):
    """Represents a point in the equity curve."""

    timestamp: datetime
    value: float


class BacktestMetrics(BaseModel):
    """Represents backtest performance metrics."""

    config: BacktestConfig
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    equity_curve: list[EquityCurvePoint]
    orders: list[Order]
    total_orders: int

    @field_validator(
        "realised_pnl",
        "unrealised_pnl",
        "total_return_pct",
        "sharpe_ratio",
        "max_drawdown",
        mode="after",
    )
    def round_values(cls, value):
        return round(value, 2)


class DeploymentConfig(BaseModel):
    """Represents deployment configuration for a strategy."""

    symbol: str
    deployment_id: UUID
    broker: BrokerType
