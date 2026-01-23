from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import Field

from core.models import CustomBaseModel
from utils.utils import get_datetime


class EventType(str, Enum):
    """Event type enumeration."""

    ORDER_PLACED = "order_placed"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_MODIFIED = "order_modified"


class BaseEvent(CustomBaseModel):
    """Base event class for all events."""

    id: UUID = Field(default_factory=uuid4)
    type: EventType
    details: dict[str, Any] | None = None
    timestamp: int = Field(default_factory=lambda: int(get_datetime().timestamp()))
