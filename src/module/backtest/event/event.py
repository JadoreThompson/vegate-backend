from enum import Enum
from typing import ClassVar, Literal
from uuid import UUID

from config import BACKTEST_EVENTS_KEY
from core.event import BaseEvent
from ..enums import BacktestStatus
from ..schema import BacktestMetricsSchema


class BacktestEventType(str, Enum):
    STATUS_CHANGED = "backtest.status_changed"
    COMPLETED = "backtest.completed"
    ERROR = "backtest.error"


class BacktestEvent(BaseEvent):
    topic: ClassVar[str] = BACKTEST_EVENTS_KEY

    type: BacktestEventType
    backtest_id: UUID


class BacktestStatusChangedEvent(BacktestEvent):
    type: Literal[BacktestEventType.STATUS_CHANGED] = BacktestEventType.STATUS_CHANGED
    status: BacktestStatus


class BacktestCompletedEvent(BacktestEvent):
    type: Literal[BacktestEventType.COMPLETED] = BacktestEventType.COMPLETED
    metrics: BacktestMetricsSchema


BacktestEventT = BacktestStatusChangedEvent | BacktestCompletedEvent
