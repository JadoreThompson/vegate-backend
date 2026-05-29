class BacktestLimitReached(Exception):
    def __init__(self):
        super().__init__("Max concurrent backtests reached")
