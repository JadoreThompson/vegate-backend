from typing import Literal
from uuid import UUID

from events.base import BaseEvent, StrategyEventType


class StrategyError(BaseEvent):
    """Event for when a strategy encounters an error."""

    type: Literal[StrategyEventType.ERROR] = StrategyEventType.ERROR
    strategy_id: UUID
    error_msg: str
