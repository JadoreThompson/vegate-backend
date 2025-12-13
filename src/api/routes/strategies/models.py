from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

from core.models import CustomBaseModel
from engine.backtesting.types import EquityCurve


class StrategyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=20)
    description: str | None = Field(None, min_length=1, max_length=250)
    prompt: str


class StrategyUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=20)
    description: str | None = Field(None, min_length=1, max_length=250)


class StrategyResponse(CustomBaseModel):
    strategy_id: UUID
    name: str
    description: str | None
    created_at: datetime
    updated_at: datetime


class StrategyDetailResponse(StrategyResponse):
    code: str
    prompt: str


class StrategyMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    equity_curve: EquityCurve


class StrategySummaryResponse(StrategyResponse):
    metrics: StrategyMetrics
