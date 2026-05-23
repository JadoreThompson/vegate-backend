from enum import Enum


class BacktestStatus(str, Enum):
    """Status of backtest execution."""

    PENDING = "pending"
    RUNNING = "running"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
