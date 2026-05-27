class StrategyCreationError(Exception):
    pass


class StrategyGenerationError(Exception):
    pass


class StrategyValidationException(Exception):

    def __init__(self, errors: list[str]):
        super().__init__()
        self._errors = tuple(errors)

    @property
    def errors(self):
        return self._errors


class StrategyNotFoundException(Exception):
    def __init__(self):
        super().__init__("Strategy not found")


class StrategyVersionNotFoundException(Exception):
    def __init__(self):
        super().__init__("Strategy version not found")


class VersionForkDetectedException(Exception):
    def __init__(self):
        super().__init__("Version fork detected: the current version has changed since you last fetched it")
