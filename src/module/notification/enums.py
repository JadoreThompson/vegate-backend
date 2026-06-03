from enum import Enum


class NotificationType(str, Enum):
    DEPLOYMENT_CAPACITY_CONSTRAINED = "deployment.capacity_constrained"
