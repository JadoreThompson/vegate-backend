from typing import Union
from uuid import UUID

from core.schema import CustomBaseModel
from .enums import NotificationType


class NotificationContext(CustomBaseModel):
    pass


class DeploymentRunningNotificationContext(NotificationContext):
    deployment_id: UUID


class DeploymentCapacityConstrainedNotificationContext(NotificationContext):
    deployment_id: UUID


class BacktestCapacityConstrainedNotificationContext(NotificationContext):
    backtest_id: UUID


NotificationContextUnion = Union[
    DeploymentRunningNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
    BacktestCapacityConstrainedNotificationContext,
]


class Notification(CustomBaseModel):
    user_id: UUID
    type: NotificationType
    context: NotificationContextUnion
