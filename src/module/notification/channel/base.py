from abc import ABC, abstractmethod
from ..schema import Notification


class NotificationChannel(ABC):

    @abstractmethod
    async def send(self, notification: Notification) -> None: ...
