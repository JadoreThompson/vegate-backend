from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, field_validator

from enums import MarketType, OrderSide, OrderType, OrderStatus, BrokerType, Timeframe


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


class OHLC(CustomBaseModel):
    """Represents an OHLC candle."""

    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: int
    timeframe: Timeframe
    symbol: str
    broker: BrokerType
    market_type: MarketType


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
