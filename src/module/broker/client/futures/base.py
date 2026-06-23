from abc import ABC, abstractmethod
from decimal import Decimal

from vegate.oms.schema import FuturesOrder, FuturesOrderRequest


class FuturesBrokerClient(ABC):
    """Abstract base class for futures broker implementations."""

    def __init__(self):
        pass

    def connect(self) -> None:
        self.get_balance()

    def disconnect(self) -> None:
        return

    @abstractmethod
    def get_balance(self) -> Decimal: ...

    @abstractmethod
    def get_equity(self) -> Decimal: ...

    @abstractmethod
    def get_position(self, symbol: str) -> Decimal: ...

    @abstractmethod
    def get_positions(self) -> list[dict]: ...

    @abstractmethod
    def place_order(self, order_request: FuturesOrderRequest) -> FuturesOrder: ...

    @abstractmethod
    def modify_order(
        self,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
        take_profit: float | None = None,
        stop_loss: float | None = None,
    ) -> FuturesOrder: ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool: ...

    @abstractmethod
    def cancel_all_orders(self) -> bool: ...

    @abstractmethod
    def get_order(self, order_id: str) -> FuturesOrder | None: ...

    @abstractmethod
    def get_orders(self) -> list[FuturesOrder]: ...

    @abstractmethod
    def set_leverage(self, symbol: str, leverage: int) -> None: ...

    @abstractmethod
    def get_leverage(self, symbol: str) -> int: ...