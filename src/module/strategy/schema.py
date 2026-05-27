from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from core.schema import CustomBaseModel


class CreateStrategyRequest(BaseModel):
    name: str
    description: str | None = None


class UpdateStrategyRequest(BaseModel):
    name: str | None = Field(None, min_length=5, max_length=20)
    description: str | None = Field(None, min_length=10, max_length=250)


class StrategyResponse(CustomBaseModel):
    id: UUID
    name: str
    description: str | None = None
    created_at: datetime
    updated_at: datetime
    cur_version_id: UUID | None = None


class StrategyCodeResponse(CustomBaseModel):
    code: str


class CreateVersionRequest(CustomBaseModel):
    prev_version_id: UUID
    code: str


class StrategyMetrics(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    total_orders: int

    @field_validator("realised_pnl", "unrealised_pnl", "total_return_pct", mode="after")
    def round_values(cls, v: float) -> float:
        return round(v, 2)


class StrategyVersionResponse(CustomBaseModel):
    id: UUID
    strategy_id: UUID
    prev_version: UUID | None = None
    code: str | None = None
    created_at: datetime
    updated_at: datetime
