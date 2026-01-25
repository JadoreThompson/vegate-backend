from datetime import datetime, date
from uuid import UUID

from pydantic import BaseModel, Field

from api.shared.models import PerformanceMetrics
from enums import BacktestStatus, BrokerType, Timeframe
from models import CustomBaseModel


class StrategyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=20)
    description: str | None = Field(None, min_length=1, max_length=250)
    prompt: str


class StrategyUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=20)
    description: str | None = Field(None, min_length=1, max_length=250)


class BacktestCreate(BaseModel):
    """Request body for creating a backtest."""

    symbol: str = Field(min_length=1, max_length=10)
    broker: str = Field(min_length=1, max_length=20)
    timeframe: Timeframe
    starting_balance: float = Field(gt=0)
    start_date: date
    end_date: date


class BacktestResponse(BaseModel):
    """Response for backtest creation."""

    backtest_id: UUID
    strategy_id: UUID
    symbol: str
    broker: BrokerType
    timeframe: Timeframe
    starting_balance: float
    start_date: date
    end_date: date
    status: BacktestStatus
    created_at: datetime


class StrategyResponse(CustomBaseModel):
    strategy_id: UUID
    name: str
    description: str | None
    created_at: datetime
    updated_at: datetime


class StrategyDetailResponse(StrategyResponse):
    code: str
    prompt: str


class StrategySummaryResponse(StrategyResponse):
    metrics: PerformanceMetrics
