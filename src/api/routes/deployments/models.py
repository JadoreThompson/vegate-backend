from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from core.enums import StrategyDeploymentStatus
from core.models import CustomBaseModel


class DeployStrategyRequest(BaseModel):
    """Request model for deploying a strategy."""

    broker_connection_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    timeframe: str = Field(min_length=1, max_length=10)


class DeploymentResponse(CustomBaseModel):
    """Response model for deployment details."""

    deployment_id: UUID
    strategy_id: UUID
    broker_connection_id: UUID
    symbol: str
    timeframe: str
    starting_balance: Decimal | None = None
    status: StrategyDeploymentStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    stopped_at: datetime | None
