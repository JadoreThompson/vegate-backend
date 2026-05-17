from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from api.shared.models import PerformanceMetrics
from enums import BrokerType, StrategyDeploymentStatus, MarketType, Timeframe, OrderStatus
from models import CustomBaseModel


class CreateDeploymentRequest(BaseModel):
    """Request model for deploying a strategy."""

    strategy_id: UUID
    broker_connection_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    timeframe: Timeframe
    market_type: MarketType
    broker_type: BrokerType


class StrategyDeploymentMetricsResponse(BaseModel):
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    profit_factor: float
    total_orders: int


class StrategyDeploymentResponse(CustomBaseModel):
    id: UUID
    strategy_id: UUID
    broker_connection_id: UUID
    symbol: str
    timeframe: Timeframe
    status: StrategyDeploymentStatus
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    stopped_at: datetime | None
    metrics: StrategyDeploymentMetricsResponse | None = None


class StrategyDeploymentDetailResponse(StrategyDeploymentResponse):
    metrics: PerformanceMetrics


class StrategyDeploymentOrderResponse(BaseModel):
    id: UUID
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
