"""
Core data models for the trading strategy framework.

This module defines the universal data models used across all broker implementations,
including order types, positions, accounts, and related enums.
"""

from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field

from core.types import OrderType, OrderSide, OrderStatus, TimeInForce


class OrderRequest(BaseModel):
    """
    Universal order request interface.

    This model standardizes order requests across all broker implementations.
    Broker-specific implementations map this to their native order formats.

    Attributes:
        symbol: Trading symbol (e.g., "AAPL", "TSLA")
        side: Buy or sell
        order_type: Type of order to place
        quantity: Number of shares (must be positive)
        limit_price: Limit price for limit orders (optional)
        stop_price: Stop price for stop orders (optional)
        time_in_force: How long the order remains active
        extended_hours: Whether to allow extended hours trading
        client_order_id: Client-specified order identifier (optional)
    """

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float = Field(gt=0)
    limit_price: float | None = Field(None, gt=0)
    stop_price: float | None = Field(None, gt=0)
    time_in_force: TimeInForce = TimeInForce.DAY
    extended_hours: bool = False
    client_order_id: str | None = None


class OrderResponse(BaseModel):
    """
    Standardized order response.

    This model represents the result of order submission or query,
    normalized across all broker implementations.

    Attributes:
        order_id: Broker-assigned order identifier
        client_order_id: Client-specified order identifier
        symbol: Trading symbol
        side: Buy or sell
        order_type: Type of order
        quantity: Total order quantity
        filled_quantity: Amount filled so far
        status: Current order status
        submitted_at: When the order was submitted
        filled_at: When the order was completely filled (if applicable)
        average_fill_price: Average price at which order was filled
        broker_metadata: Broker-specific additional data
    """

    order_id: str
    client_order_id: Optional[str] = None
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    filled_quantity: float = 0.0
    status: OrderStatus
    submitted_at: datetime
    filled_at: Optional[datetime] = None
    average_fill_price: Optional[float] = None
    broker_metadata: dict[str, Any] = Field(default_factory=dict)


class Position(BaseModel):
    """
    Current position information for a symbol.

    Represents the current holdings and profit/loss for a particular symbol.

    Attributes:
        symbol: Trading symbol
        quantity: Number of shares held (positive for long, negative for short)
        average_entry_price: Average price at which position was entered
        current_price: Current market price
        market_value: Current market value of position
        unrealized_pnl: Unrealized profit/loss in dollars
        unrealized_pnl_percent: Unrealized profit/loss as percentage
        cost_basis: Total cost of the position
        side: Whether position is long (BUY) or short (SELL)
    """

    symbol: str
    quantity: float
    average_entry_price: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_percent: float
    cost_basis: float
    side: OrderSide


class Account(BaseModel):
    """
    Account information and balances.

    Represents the current state of the trading account including
    cash, equity, and buying power.

    Attributes:
        account_id: Broker account identifier
        equity: Total account equity (cash + positions)
        cash: Available cash balance
        buying_power: Available buying power (may include margin)
        portfolio_value: Total portfolio value
        last_updated: When this data was last updated
    """

    account_id: str
    equity: float
    cash: float
    buying_power: float
    portfolio_value: float
    last_updated: datetime
