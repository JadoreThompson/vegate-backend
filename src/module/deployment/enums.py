from enum import Enum


class StrategyDeploymentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    STOP_REQUESTED = "stop_requested"
    SUSPICIOUS = "suspicious"
