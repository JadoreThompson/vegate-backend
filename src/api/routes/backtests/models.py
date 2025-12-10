from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from core.enums import BacktestStatus
from core.models import CustomBaseModel
from engine.enums import Timeframe


class BacktestCreate(BaseModel):
    strategy_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    starting_balance: Decimal = Field(gt=0)
    timeframe: Timeframe
    start_date: date
    end_date: date


class BacktestUpdate(BaseModel):
    status: BacktestStatus | None = None
    metrics: dict | None = None


class BacktestResponse(CustomBaseModel):
    backtest_id: UUID
    strategy_id: UUID
    symbol: str
    starting_balance: Decimal
    status: BacktestStatus
    created_at: datetime


class BacktestMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    total_trades: int


class BacktestDetailResponse(BacktestResponse):
    metrics: BacktestMetrics | None


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
