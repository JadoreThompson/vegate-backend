from typing import Union
from uuid import UUID

from core.schema import CustomBaseModel
from .enums import NotificationType


class NotificationContext(CustomBaseModel):
    pass


class DeploymentCapacityConstrainedNotificationContext(NotificationContext):
    deployment_id: UUID


NotificationContextUnion = Union[DeploymentCapacityConstrainedNotificationContext]


class Notification(CustomBaseModel):
    user_id: UUID
    type: NotificationType
    context: NotificationContextUnion
