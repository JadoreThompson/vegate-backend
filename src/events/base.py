from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import Field

from models import CustomBaseModel
from utils import get_datetime


class BaseEvent(CustomBaseModel):
    """Base event class for all events."""

    id: UUID = Field(default_factory=uuid4)
    type: Enum
    details: dict[str, Any] | None = None
    timestamp: int = Field(default_factory=lambda: int(get_datetime().timestamp()))
