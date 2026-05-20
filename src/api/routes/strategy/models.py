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


class StrategyResponse(CustomBaseModel):
    id: UUID
    name: str
    description: str
    prompt: str
    created_at: datetime
    updated_at: datetime
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
