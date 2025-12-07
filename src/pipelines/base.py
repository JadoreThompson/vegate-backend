from abc import ABC, abstractmethod


class BasePipeline(ABC):

    @abstractmethod
    async def run_stocks_pipeline(self, symbol: str) -> None: ...

    @abstractmethod
    async def run_crypto_pipeline(self, symbol: str) -> None: ...

    @abstractmethod
    async def __aenter__(self): ...

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, tcb): ...
