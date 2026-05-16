from datetime import datetime, date
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from api.shared.models import PerformanceMetrics
from enums import BacktestStatus, BrokerType, Timeframe
from models import CustomBaseModel


class CreateStrategyRequest(BaseModel):
    description: str


class UpdateStrategyRequest(BaseModel):
    name: str | None = Field(None, min_length=5, max_length=20)
    description: str | None = Field(None, min_length=10, max_length=250)


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
    id: UUID
    name: str
    description: str
    prompt: str
    created_at: datetime
    updated_at: datetime


class StrategyDetailResponse(StrategyResponse):
    code: str
    prompt: str


class StrategySummaryResponse(StrategyResponse):
    metrics: PerformanceMetrics


class StrategyMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    total_orders: int

    @field_validator("realised_pnl", "unrealised_pnl", "total_return_pct", mode="after")
    def round_values(cls, v: float) -> float:
        return round(v, 2)