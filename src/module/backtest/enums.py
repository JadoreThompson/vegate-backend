from enum import Enum


class BacktestStatus(str, Enum):
    """Status of backtest execution."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUSPICIOUS = "suspicious"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestCancellationReason(str, Enum):
    CAPACITY_CONSTRAINT = "capacity_constraint"
