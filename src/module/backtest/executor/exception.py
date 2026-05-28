class BacktestExistsException(Exception):
    def __init__(self, backtest_id: str):
        self.backtest_id = backtest_id
        super().__init__(f"Backtest with id '{backtest_id}' already exists.")
