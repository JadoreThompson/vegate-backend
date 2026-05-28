class BacktestNotFoundException(Exception):

    def __init__(self, backtest_id):
        super().__init__(f"Backtest with id '{backtest_id}' not found.")


class BacktestMetricsNotFoundException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class InvalidDateRange(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class BacktestInProgressException(Exception):

    def __init__(self):
        super().__init__("Backtest is currently in progress.")
