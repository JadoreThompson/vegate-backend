from datetime import datetime, date
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from enums import BacktestStatus, BrokerType, MarketType, OrderStatus
from models import CustomBaseModel
from enums import Timeframe


class CreateBacktestRequest(BaseModel):
    strategy_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    broker: BrokerType
    market_type: MarketType
    starting_balance: int = Field(gt=0, le=100_000)
    start_date: datetime
    end_date: datetime
    timeframe: Timeframe


class CreateBacktestResponse(BaseModel):
    id: UUID


class BacktestMetricsResponse(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    total_orders: int


class BacktestResponse(CustomBaseModel):
    id: UUID
    strategy_id: UUID
    symbol: str
    broker: BrokerType
    market_type: MarketType
    starting_balance: float
    start_date: date
    end_date: date
    status: BacktestStatus
    created_at: datetime
    metrics: BacktestMetricsResponse | None = None


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
