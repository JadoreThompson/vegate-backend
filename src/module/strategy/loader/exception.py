class StrategyLoadError(Exception):
    def __init__(self, message: str, exc: Exception | None = None):
        super().__init__(message)
        self.exc = exc
