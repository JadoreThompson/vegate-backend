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


class BacktestRunningNotificationContext(NotificationContext):
    backtest_id: UUID


class BacktestCompletedNotificationContext(NotificationContext):
    backtest_id: UUID


class BacktestFailedNotificationContext(NotificationContext):
    backtest_id: UUID


class BacktestCapacityConstrainedNotificationContext(NotificationContext):
    backtest_id: UUID


NotificationContextUnion = Union[
    DeploymentRunningNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
    BacktestRunningNotificationContext,
    BacktestCompletedNotificationContext,
    BacktestFailedNotificationContext,
    BacktestCapacityConstrainedNotificationContext,
]


class Notification(CustomBaseModel):
    user_id: UUID
    type: NotificationType
    context: NotificationContextUnion
