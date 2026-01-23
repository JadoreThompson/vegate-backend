from datetime import datetime

from pydantic import BaseModel

from enums import OrderType, OrderStatus


class OrderRequest(BaseModel):
    """Represents an order request."""

    symbol: str
    quantity: float
    notional: float
    order_type: OrderType
    price: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    executed_at: datetime | None = None
    submitted_at: datetime | None = None


class Order(BaseModel):
    """Represents a trading order."""

    order_id: str
    symbol: str
    quantity: float
    notional: float
    order_type: OrderType
    price: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    executed_at: datetime | None = None
    submitted_at: datetime | None = None
    status: OrderStatus = OrderStatus.PENDING
    details: dict[str, object] | None = None


class OHLC(BaseModel):
    """Represents an OHLC candle."""

    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    timeframe: str
    symbol: str


class Tick(BaseModel):
    """Represents a single tick."""

    symbol: str
    timestamp: datetime
    price: float


class BacktestMetrics(BaseModel):
    """Represents backtest performance metrics."""

    total_pnl: float
    highest_balance: float
    lowest_balance: float
    start_date: datetime
    end_date: datetime
    symbol: str
    orders: list[Order]
    starting_balance: float
    ending_balance: float
    total_return_percent: float
    num_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
