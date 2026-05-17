class SymbolNotFoundException(Exception):

    def __init__(self, symbol: str):
        super().__init__(f"Symbol '{symbol}' not found.")


class BacktestNotFoundException(Exception):

    def __init__(self):
        super().__init__(f"Backtest not found.")


class BacktestMetricsNotFoundException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class InvalidDateRange(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class BacktestInProgressError(Exception):

    def __init__(self):
        super().__init__("Backtest is currently in progress.")
