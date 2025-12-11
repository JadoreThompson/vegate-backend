from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from api.shared.models import PerformanceMetrics
from core.enums import StrategyDeploymentStatus
from core.models import CustomBaseModel
from engine.enums import MarketType


class DeployStrategyRequest(BaseModel):
    """Request model for deploying a strategy."""

    broker_connection_id: UUID
    market_type: MarketType
    symbol: str = Field(min_length=1, max_length=10)
    timeframe: str = Field(min_length=1, max_length=10)


class DeploymentResponse(CustomBaseModel):
    """Response model for deployment details."""

    deployment_id: UUID
    strategy_id: UUID
    broker_connection_id: UUID
    market_type: MarketType
    symbol: str
    timeframe: str
    starting_balance: Decimal | None = None
    status: StrategyDeploymentStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    stopped_at: datetime | None


class DeploymentDetailResponse(DeploymentResponse):
    metrics: PerformanceMetrics
