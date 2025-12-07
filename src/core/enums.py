from enum import Enum


class PricingTierType(str, Enum):
    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class BacktestStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class StratgyDeploymentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    ERROR = "error"
