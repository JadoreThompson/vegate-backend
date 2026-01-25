from typing import Any
from uuid import UUID

from core.enums import DeploymentEventType
from core.models import OHLCV, CustomBaseModel
from enums import BrokerType


class DeploymentEvent(CustomBaseModel):
    type: DeploymentEventType
    deployment_id: UUID


class BrokerTradeEvent(CustomBaseModel):
    """
    Emitted via the listener, contains the details
    for the trade. To be consumed to build ohlc levels
    and update the in memory cache of instrument prices
    """

    broker: BrokerType
    symbol: str
    price: float
    quantity: float
    timestamp: int
    broker_metadata: dict[str, Any] | None = None


class CandleCloseEvent(OHLCV):
    broker: BrokerType
