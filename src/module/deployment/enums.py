from enum import Enum


class StrategyDeploymentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    STOP_REQUESTED = "stop_requested"
    SUSPICIOUS = "suspicious"
    CANCELLED = "cancelled"


class DeploymentCancellationReason(str, Enum):
    CAPACITY_CONSTRAINT = "capacity_constraint"
