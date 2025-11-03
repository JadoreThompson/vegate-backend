from typing import Any
from uuid import UUID, uuid4

from pydantic import Field

from core.enums import CoreEventType
from core.models import CustomBaseModel


class CoreEvent(CustomBaseModel):
    type: CoreEventType
    data: Any
    id: UUID = Field(default_factory=uuid4)
