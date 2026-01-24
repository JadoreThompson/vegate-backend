from multiprocessing.queues import Queue


# Global queue instance - will be set by the backend runner
_backtest_queue: Queue | None = None


def set_backtest_queue(queue: Queue) -> None:
    """Set the global backtest queue.

    Args:
        queue: The multiprocessing Queue to use for backtest jobs
    """
    global _backtest_queue
    _backtest_queue = queue


def get_backtest_queue() -> Queue | None:
    """Get the global backtest queue.

    Returns:
        The backtest queue, or None if not set
    """
    return _backtest_queue
