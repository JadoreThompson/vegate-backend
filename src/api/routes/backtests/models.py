from datetime import datetime, date
from uuid import UUID

from pydantic import BaseModel, Field

from enums import BacktestStatus, BrokerType
from models import CustomBaseModel
from enums import Timeframe
from models import EquityCurvePoint


class BacktestCreate(BaseModel):
    strategy_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    broker: BrokerType
    starting_balance: int = Field(gt=0, le=100_000)
    start_date: date
    end_date: date
    timeframe: Timeframe


class BacktestResponse(CustomBaseModel):
    backtest_id: UUID
    strategy_id: UUID
    symbol: str
    starting_balance: float
    status: BacktestStatus
    created_at: datetime


class BacktestMetricsResponse(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    total_orders: int
    equity_curve: list[EquityCurvePoint]


class BacktestDetailResponse(BacktestResponse):
    metrics: BacktestMetricsResponse | None
