from enum import Enum


class NotificationStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class NotificationType(str, Enum):
    DEPLOYMENT_RUNNING = "deployment.running"
    DEPLOYMENT_CAPACITY_CONSTRAINED = "deployment.capacity_constrained"
    BACKTEST_CAPACITY_CONSTRAINED = "backtest.capacity_constrained"
