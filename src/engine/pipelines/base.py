from abc import ABC, abstractmethod


class BasePipeline(ABC):
    
    @abstractmethod
    async def run_stocks_pipeline(self, symbol: str) -> None: ...
    
    @abstractmethod
    async def run_crypto_pipeline(self, symbol: str) -> None: ...
