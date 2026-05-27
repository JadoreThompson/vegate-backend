from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from module.broker.enums import OrderStatus
from module.deployment.enums import StrategyDeploymentStatus
from core.schema import CustomBaseModel


class CreateDeploymentRequest(BaseModel):
    """Request model for deploying a strategy."""

    version_id: UUID
    broker_connection_id: UUID

class CreateStrategyDeploymentResponse(CustomBaseModel):
    id: UUID


class StrategyDeploymentMetricsResponse(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    total_orders: int


class StrategyDeploymentResponse(CustomBaseModel):
    id: UUID
    version_id: UUID
    broker_connection_id: UUID
    status: StrategyDeploymentStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    stopped_at: datetime | None
    metrics: StrategyDeploymentMetricsResponse | None = None


class StrategyDeploymentOrderResponse(BaseModel):
    id: UUID
    broker_order_id: UUID | None = None
    deployment_id: UUID
    symbol: str
    side: str
    order_type: str
    quantity: float | None = None
    notional: float | None = None
    filled_quantity: float = 0
    limit_price: float | None = None
    stop_price: float | None = None
    avg_fill_price: float | None = None
    status: OrderStatus
    filled_at: datetime | None = None
    details: dict[str, Any] | None = None
    created_at: datetime
    candle_ts: int
