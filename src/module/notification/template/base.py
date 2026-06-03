from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from ..schema import Notification

T = TypeVar("T")


class NotificationTemplateEngine(ABC, Generic[T]):

    @abstractmethod
    def render(self, notification: Notification, recipient: str) -> T: ...
