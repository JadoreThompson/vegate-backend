from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from core.enums import BacktestStatus
from core.models import CustomBaseModel
from engine.backtesting import EquityCurveT
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
    equity_curve: EquityCurveT


class BacktestDetailResponse(BacktestResponse):
    metrics: BacktestMetrics | None

