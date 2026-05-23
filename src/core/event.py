from enum import Enum
from typing import Any, ClassVar
from uuid import UUID, uuid4

from pydantic import Field

from core.schema import CustomBaseModel
from util import get_datetime


class BaseEvent(CustomBaseModel):
    """Base event class for all events."""

    topic: ClassVar[str]

    id: UUID = Field(default_factory=uuid4)
    type: Enum
    details: dict[str, Any] | None = None
    timestamp: int = Field(default_factory=lambda: int(get_datetime().timestamp()))
