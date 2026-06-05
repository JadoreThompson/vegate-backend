from enum import Enum
from typing import ClassVar, Literal
from uuid import UUID

from config import BACKTEST_EVENTS_KEY
from core.event import BaseEvent
from ..enums import BacktestStatus
from ..schema import BacktestMetricsSchema


class BacktestEventType(str, Enum):
    STATUS_CHANGED = "backtest.status_changed"
    ERROR = "backtest.error"
    REQUESTED = "backtest.requested"
    STOP_REQUESTED = "backtest.stop_requested"
    CANCELLED = "backtest.cancelled"


class BacktestEvent(BaseEvent):
    topic: ClassVar[str] = BACKTEST_EVENTS_KEY
    type: BacktestEventType
    backtest_id: UUID


class BacktestStatusChangedEvent(BacktestEvent):
    type: Literal[BacktestEventType.STATUS_CHANGED] = BacktestEventType.STATUS_CHANGED
    status: BacktestStatus


class BacktestRequestedEvent(BacktestEvent):
    type: Literal[BacktestEventType.REQUESTED] = BacktestEventType.REQUESTED


class BacktestStopRequestedEvent(BacktestEvent):
    type: Literal[BacktestEventType.STOP_REQUESTED] = BacktestEventType.STOP_REQUESTED


class BacktestCancelledEvent(BacktestEvent):
    type: Literal[BacktestEventType.CANCELLED] = BacktestEventType.CANCELLED
    reason: str


BacktestEventT = (
    BacktestStatusChangedEvent
    | BacktestRequestedEvent
    | BacktestStopRequestedEvent
    | BacktestCancelledEvent
)
