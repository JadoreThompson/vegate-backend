# Trading Strategy Framework Architecture

**Version:** 1.0  
**Last Updated:** December 2025  
**Status:** Design Specification

## Table of Contents

1. [Overview](#overview)
2. [System Requirements](#system-requirements)
3. [Architectural Principles](#architectural-principles)
4. [Module 1: Broker System Architecture](#module-1-broker-system-architecture)
5. [Module 2: Backtesting Engine Architecture](#module-2-backtesting-engine-architecture)
6. [Module 3: Context Object Architecture](#module-3-context-object-architecture)
7. [Integration & Data Flow](#integration--data-flow)
8. [Extension Points](#extension-points)
9. [Error Handling Strategy](#error-handling-strategy)
10. [Performance Considerations](#performance-considerations)

---

## Overview

This document defines the architecture for a trading strategy framework that converts natural language descriptions into executable Python trading strategies. The system supports multiple brokers, backtesting capabilities, and provides comprehensive runtime context to strategies.

### Key Design Goals

- **Broker Abstraction**: Unified interface for multiple broker platforms (Alpaca, Interactive Brokers, etc.)
- **Async-First**: Event-driven architecture using async/await for low-latency execution
- **Extensibility**: Plugin-based design for easy addition of new brokers and data sources
- **Testability**: Clear separation between live trading and backtesting with shared interfaces
- **Type Safety**: Comprehensive type hints and validation using Pydantic models

### Technology Stack

- **Language**: Python 3.11+
- **Async Framework**: asyncio
- **Database**: PostgreSQL for OHLC data storage
- **Data Validation**: Pydantic v2
- **Primary Broker**: Alpaca (initial implementation)

---

## System Requirements

### Functional Requirements

1. Support async order execution across multiple broker platforms
2. Provide backtesting with realistic order fills including slippage simulation
3. Support timeframes from 1-minute to daily bars
4. Expose runtime context with OHLC data, volume, and historical access
5. Handle authentication lifecycle (login/logout) for broker connections
6. Implement rate limiting and error recovery for broker APIs

### Non-Functional Requirements

1. **Latency**: < 100ms for order submission in live trading
2. **Throughput**: Support 100+ concurrent strategy instances
3. **Reliability**: 99.9% uptime for live trading connections
4. **Data Consistency**: Ensure accurate historical data replay in backtests
5. **Scalability**: Horizontal scaling for multiple strategy executions

---

## Architectural Principles

### Design Patterns

1. **Abstract Factory Pattern**: For broker instantiation
2. **Strategy Pattern**: For different execution modes (live/backtest)
3. **Observer Pattern**: For market data updates and event handling
4. **Dependency Injection**: For testability and loose coupling
5. **Context Manager Protocol**: For resource lifecycle management

### Key Principles

- **Single Responsibility**: Each class has one clear purpose
- **Open/Closed**: Open for extension, closed for modification
- **Liskov Substitution**: All broker implementations are interchangeable
- **Interface Segregation**: Minimal, focused interfaces
- **Dependency Inversion**: Depend on abstractions, not concrete implementations

---

## Module 1: Broker System Architecture

### Overview

The broker system provides a unified abstraction layer over multiple broker APIs. It handles authentication, order management, position tracking, and market data subscription.

### Class Hierarchy

```
BaseBroker (ABC)
├── AlpacaBroker
├── InteractiveBrokersBroker
└── PaperTradingBroker
```

### Core Data Models

```python
from enum import Enum
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(str, Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


class OrderRequest(BaseModel):
    """Universal order request interface"""
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float = Field(gt=0)
    limit_price: Optional[float] = Field(None, gt=0)
    stop_price: Optional[float] = Field(None, gt=0)
    time_in_force: TimeInForce = TimeInForce.DAY
    extended_hours: bool = False
    client_order_id: Optional[str] = None


class OrderResponse(BaseModel):
    """Standardized order response"""
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
    broker_metadata: Dict[str, Any] = Field(default_factory=dict)


class Position(BaseModel):
    """Current position information"""
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
    """Account information"""
    account_id: str
    equity: float
    cash: float
    buying_power: float
    portfolio_value: float
    last_updated: datetime
```

### BaseBroker Interface

```python
from abc import ABC, abstractmethod
from typing import Optional, List


class BaseBroker(ABC):
    """
    Abstract base class for all broker implementations.

    Lifecycle:
        1. __init__() - Initialize with credentials
        2. connect() - Establish connection and authenticate
        3. ... use broker methods ...
        4. disconnect() - Clean up resources
    """

    # Lifecycle Management

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to broker and authenticate"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully disconnect from broker"""
        pass

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        return False

    # Order Management

    @abstractmethod
    async def submit_order(self, order: OrderRequest) -> OrderResponse:
        """Submit an order to the broker"""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        pass

    @abstractmethod
    async def get_order(self, order_id: str) -> OrderResponse:
        """Get current status of an order"""
        pass

    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderResponse]:
        """Get all open orders"""
        pass

    # Position Management

    @abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get current position for a symbol"""
        pass

    @abstractmethod
    async def get_all_positions(self) -> List[Position]:
        """Get all current positions"""
        pass

    @abstractmethod
    async def close_position(self, symbol: str) -> OrderResponse:
        """Close entire position for a symbol"""
        pass

    # Account Information

    @abstractmethod
    async def get_account(self) -> Account:
        """Get current account information"""
        pass
```

### Rate Limiting Strategy

```python
import asyncio
from time import time


class TokenBucketRateLimiter:
    """Token bucket rate limiter for broker API calls"""

    def __init__(self, rate: int, per_seconds: int = 60):
        self.rate = rate
        self.per_seconds = per_seconds
        self.allowance = rate
        self.last_check = time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire permission to make a request"""
        async with self._lock:
            current = time()
            time_passed = current - self.last_check
            self.last_check = current

            self.allowance += time_passed * (self.rate / self.per_seconds)
            if self.allowance > self.rate:
                self.allowance = self.rate

            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per_seconds / self.rate)
                await asyncio.sleep(sleep_time)
                self.allowance = 0.0
            else:
                self.allowance -= 1.0
```

### Broker Factory

```python
from typing import Type, Dict


class BrokerFactory:
    """Factory for creating broker instances"""

    _brokers: Dict[str, Type[BaseBroker]] = {}

    @classmethod
    def register(cls, name: str, broker_class: Type[BaseBroker]) -> None:
        """Register a new broker type"""
        cls._brokers[name] = broker_class

    @classmethod
    def create(cls, name: str, credentials: BrokerCredentials) -> BaseBroker:
        """Create a broker instance"""
        if name not in cls._brokers:
            raise ValueError(f"Unknown broker: {name}")
        return cls._brokers[name](credentials)
```

### Error Handling

```python
class BrokerError(Exception):
    """Base exception for broker-related errors"""
    def __init__(self, message: str, broker_code: Optional[str] = None,
                 retry_after: Optional[int] = None):
        super().__init__(message)
        self.broker_code = broker_code
        self.retry_after = retry_after


class AuthenticationError(BrokerError):
    """Raised when authentication fails"""
    pass


class OrderRejectedError(BrokerError):
    """Raised when an order is rejected"""
    pass


class RateLimitError(BrokerError):
    """Raised when rate limit is exceeded"""
    pass


class InsufficientFundsError(BrokerError):
    """Raised when account has insufficient funds"""
    pass
```

---

## Module 2: Backtesting Engine Architecture

### Overview

The backtesting engine simulates strategy execution over historical data. It replays OHLC bars, simulates order fills with realistic slippage, tracks portfolio state, and generates performance metrics.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│              Backtesting Engine                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────┐      ┌──────────────┐           │
│  │ Data Loader  │─────▶│ Event Queue  │           │
│  └──────────────┘      └──────────────┘           │
│         │                      │                   │
│         │                      ▼                   │
│         │              ┌──────────────┐           │
│         │              │ Sim. Broker  │           │
│         │              └──────────────┘           │
│         │                      │                   │
│         ▼                      ▼                   │
│  ┌──────────────┐      ┌──────────────┐           │
│  │   Context    │◀─────│  Portfolio   │           │
│  │   Manager    │      │   Tracker    │           │
│  └──────────────┘      └──────────────┘           │
│         │                      │                   │
│         ▼                      ▼                   │
│  ┌──────────────┐      ┌──────────────┐           │
│  │   Strategy   │      │   Metrics    │           │
│  │  Executor    │      │  Calculator  │           │
│  └──────────────┘      └──────────────┘           │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Core Data Models

```python
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional


class Timeframe(str, Enum):
    """Supported timeframes for OHLC data"""
    MINUTE_1 = "1min"
    MINUTE_5 = "5min"
    MINUTE_15 = "15min"
    MINUTE_30 = "30min"
    HOUR_1 = "1hour"
    DAY_1 = "1day"


@dataclass
class OHLCBar:
    """Single OHLC bar"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    timeframe: Timeframe


@dataclass
class BacktestConfig:
    """Configuration for backtest execution"""
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    commission_per_share: float = 0.0
    commission_percent: float = 0.0
    slippage_percent: float = 0.1
    timeframe: Timeframe = Timeframe.MINUTE_1
    allow_fractional_shares: bool = False
    enable_slippage: bool = True
    enable_commissions: bool = True
    symbols: List[str] = None


@dataclass
class BacktestResult:
    """Results from backtest execution"""
    config: BacktestConfig
    initial_capital: float
    final_capital: float
    total_return: float
    total_return_percent: float
    sharpe_ratio: float
    max_drawdown: float
    max_drawdown_percent: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    equity_curve: List[tuple[datetime, float]]
    trades: List['TradeRecord']
    execution_time_seconds: float


@dataclass
class TradeRecord:
    """Record of a single trade execution"""
    trade_id: str
    symbol: str
    side: OrderSide
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    quantity: float = 0.0
    pnl: float = 0.0
    commission: float = 0.0
    slippage: float = 0.0
```

### Data Loader

```python
from typing import AsyncIterator


class OHLCDataLoader:
    """Loads historical OHLC data from PostgreSQL database"""

    def __init__(self, db_connection):
        self.db = db_connection

    async def load_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: Timeframe,
        batch_size: int = 10000
    ) -> AsyncIterator[List[OHLCBar]]:
        """
        Load OHLC data in batches.

        Yields batches of OHLC bars in chronological order
        """
        query = """
            SELECT symbol, timestamp, open, high, low, close, volume
            FROM ohlc_data
            WHERE symbol = ANY($1)
              AND timestamp >= $2
              AND timestamp <= $3
              AND timeframe = $4
            ORDER BY timestamp ASC, symbol ASC
            LIMIT $5 OFFSET $6
        """

        offset = 0
        while True:
            rows = await self.db.fetch(
                query, symbols, start_date, end_date,
                timeframe.value, batch_size, offset
            )

            if not rows:
                break

            bars = [
                OHLCBar(
                    symbol=row['symbol'],
                    timestamp=row['timestamp'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume'],
                    timeframe=timeframe
                )
                for row in rows
            ]

            yield bars
            offset += batch_size
```

### Simulated Broker

```python
from typing import Dict
import uuid


class SimulatedBroker(BaseBroker):
    """
    Simulated broker for backtesting.

    Implements BaseBroker interface but simulates order fills
    using historical data with realistic slippage.
    """

    def __init__(
        self,
        initial_capital: float,
        commission_per_share: float = 0.0,
        commission_percent: float = 0.0,
        slippage_percent: float = 0.1
    ):
        self.cash = initial_capital
        self.commission_per_share = commission_per_share
        self.commission_percent = commission_percent
        self.slippage_percent = slippage_percent

        self.positions: Dict[str, Position] = {}
        self.orders: Dict[str, OrderResponse] = {}
        self.current_time: Optional[datetime] = None
        self.current_prices: Dict[str, float] = {}
        self._order_counter = 0

    async def connect(self) -> None:
        """No real connection needed"""
        pass

    async def disconnect(self) -> None:
        """No real disconnection needed"""
        pass

    def set_current_time(self, timestamp: datetime) -> None:
        """Update simulation time"""
        self.current_time = timestamp

    def set_current_price(self, symbol: str, price: float) -> None:
        """Update current market price"""
        self.current_prices[symbol] = price

    async def submit_order(self, order: OrderRequest) -> OrderResponse:
        """Simulate immediate order fill with slippage"""
        if order.symbol not in self.current_prices:
            raise BrokerError(f"No price data for {order.symbol}")

        # Calculate fill price with slippage
        base_price = self.current_prices[order.symbol]
        slippage_factor = 1 + (self.slippage_percent / 100)

        if order.side == OrderSide.BUY:
            fill_price = base_price * slippage_factor
        else:
            fill_price = base_price / slippage_factor

        # Calculate commission
        commission = (
            order.quantity * self.commission_per_share +
            fill_price * order.quantity * (self.commission_percent / 100)
        )

        # Check buying power
        if order.side == OrderSide.BUY:
            required_cash = fill_price * order.quantity + commission
            if required_cash > self.cash:
                raise InsufficientFundsError(
                    f"Insufficient funds: need {required_cash}, have {self.cash}"
                )

        # Execute trade
        self._order_counter += 1
        order_id = f"SIM{self._order_counter:08d}"

        if order.side == OrderSide.BUY:
            self.cash -= (fill_price * order.quantity + commission)
            self._update_position(order.symbol, order.quantity, fill_price)
        else:
            self.cash += (fill_price * order.quantity - commission)
            self._update_position(order.symbol, -order.quantity, fill_price)

        response = OrderResponse(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity,
            filled_quantity=order.quantity,
            status=OrderStatus.FILLED,
            submitted_at=self.current_time,
            filled_at=self.current_time,
            average_fill_price=fill_price,
            broker_metadata={"commission": commission, "slippage": fill_price - base_price}
        )

        self.orders[order_id] = response
        return response

    def _update_position(self, symbol: str, quantity_delta: float, price: float) -> None:
        """Update position after trade"""
        if symbol in self.positions:
            pos = self.positions[symbol]
            new_quantity = pos.quantity + quantity_delta

            if new_quantity == 0:
                del self.positions[symbol]
            else:
                new_cost_basis = (pos.cost_basis + quantity_delta * price)
                pos.quantity = new_quantity
                pos.cost_basis = new_cost_basis
                pos.average_entry_price = new_cost_basis / new_quantity
        else:
            if quantity_delta != 0:
                self.positions[symbol] = Position(
                    symbol=symbol,
                    quantity=quantity_delta,
                    average_entry_price=price,
                    current_price=price,
                    market_value=quantity_delta * price,
                    unrealized_pnl=0.0,
                    unrealized_pnl_percent=0.0,
                    cost_basis=quantity_delta * price,
                    side=OrderSide.BUY if quantity_delta > 0 else OrderSide.SELL
                )

    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get current position"""
        return self.positions.get(symbol)

    async def get_all_positions(self) -> List[Position]:
        """Get all positions"""
        return list(self.positions.values())

    async def get_account(self) -> Account:
        """Get account info"""
        portfolio_value = self.cash + sum(
            pos.market_value for pos in self.positions.values()
        )

        return Account(
            account_id="SIMULATED",
            equity=portfolio_value,
            cash=self.cash,
            buying_power=self.cash,
            portfolio_value=portfolio_value,
            last_updated=self.current_time
        )
```

### Backtesting Engine

```python
import time as time_module
from collections import defaultdict


class BacktestEngine:
    """Main backtesting engine that orchestrates the simulation"""

    def __init__(
        self,
        config: BacktestConfig,
        data_loader: OHLCDataLoader,
        strategy_func: Callable
    ):
        self.config = config
        self.data_loader = data_loader
        self.strategy_func = strategy_func

        # Initialize simulated broker
        self.broker = SimulatedBroker(
            initial_capital=config.initial_capital,
            commission_per_share=config.commission_per_share,
            commission_percent=config.commission_percent,
            slippage_percent=config.slippage_percent
        )

        # Tracking
        self.equity_curve: List[tuple[datetime, float]] = []
        self.trades: List[TradeRecord] = []
        self.current_bars: Dict[str, OHLCBar] = {}

    async def run(self) -> BacktestResult:
        """Execute backtest"""
        start_time = time_module.time()

        await self.broker.connect()

        # Load and process data
        async for batch in self.data_loader.load_data(
            symbols=self.config.symbols,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            timeframe=self.config.timeframe
        ):
            # Group bars by timestamp
            bars_by_time = defaultdict(list)
            for bar in batch:
                bars_by_time[bar.timestamp].append(bar)

            # Process each timestamp
            for timestamp in sorted(bars_by_time.keys()):
                bars = bars_by_time[timestamp]
                await self._process_bars(timestamp, bars)

        await self.broker.disconnect()

        # Calculate metrics
        execution_time = time_module.time() - start_time
        result = await self._calculate_results(execution_time)

        return result

    async def _process_bars(self, timestamp: datetime, bars: List[OHLCBar]) -> None:
        """Process bars at a single timestamp"""
        # Update broker state
        self.broker.set_current_time(timestamp)
        for bar in bars:
            self.broker.set_current_price(bar.symbol, bar.close)
            self.current_bars[bar.symbol] = bar

        # Create context for strategy
        context = StrategyContext(
            timestamp=timestamp,
            bars=self.current_bars,
            broker=self.broker,
            data_loader=self.data_loader
        )

        # Execute strategy
        await self.strategy_func(context)

        # Record equity
        account = await self.broker.get_account()
        self.equity_curve.append((timestamp, account.portfolio_value))

    async def _calculate_results(self, execution_time: float) -> BacktestResult:
        """Calculate performance metrics"""
        account = await self.broker.get_account()

        total_return = account.portfolio_value - self.config.initial_capital
        total_return_pct = (total_return / self.config.initial_capital) * 100

        # Calculate additional metrics
        sharpe = self._calculate_sharpe_ratio()
        max_dd, max_dd_pct = self._calculate_max_drawdown()

        # Trade statistics
        total_trades = len(self.trades)
        winning_trades = sum(1 for t in self.trades if t.pnl > 0)
        losing_trades = sum(1 for t in self.trades if t.pnl < 0)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        return BacktestResult(
            config=self.config,
            initial_capital=self.config.initial_capital,
            final_capital=account.portfolio_value,
            total_return=total_return,
            total_return_percent=total_return_pct,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            max_drawdown_percent=max_dd_pct,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            equity_curve=self.equity_curve,
            trades=self.trades,
            execution_time_seconds=execution_time
        )

    def _calculate_sharpe_ratio(self) -> float:
        """Calculate Sharpe ratio from equity curve"""
        if len(self.equity_curve) < 2:
            return 0.0

        returns = []
        for i in range(1, len(self.equity_curve)):
            prev_equity = self.equity_curve[i-1][1]
            curr_equity = self.equity_curve[i][1]
            ret = (curr_equity - prev_equity) / prev_equity
            returns.append(ret)

        if not returns:
            return 0.0

        import numpy as np
        mean_return = np.mean(returns)
        std_return = np.std(returns)

        if std_return == 0:
            return 0.0

        # Annualize assuming 252 trading days
        sharpe = (mean_return / std_return) * np.sqrt(252)
        return sharpe

    def _calculate_max_drawdown(self) -> tuple[float, float]:
        """Calculate maximum drawdown"""
        if not self.equity_curve:
            return 0.0, 0.0

        peak = self.equity_curve[0][1]
        max_dd = 0.0

        for _, equity in self.equity_curve:
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0.0
        return max_dd, max_dd_pct
```

---

## Module 3: Context Object Architecture

### Overview

The context object provides strategies with runtime access to market data, historical information, and broker operations. It abstracts the differences between live trading and backtesting, presenting a unified interface.

### Design Philosophy

- **Single Source of Truth**: Context is the only interface strategies need
- **Immutable Per Bar**: Context state reflects a single point in time
- **Lazy Loading**: Historical data fetched on-demand
- **Type-Safe**: Full type hints for IDE autocomplete

### Core Context Interface

````python
from typing import Optional, List, Dict
from datetime import datetime, timedelta


class StrategyContext:
    """
    Runtime context provided to trading strategies.

    Provides access to:
    - Current market data (OHLC, volume)
    - Historical data
    - Broker operations
    - Position and account information

    Example usage in strategy:
        async def my_strategy(ctx: StrategyContext):
            # Get current close price
            price = ctx.close('AAPL')

            # Get historical data
            history = await ctx.history('AAPL', bars=20)

            # Check position
            position = await ctx.position('AAPL')

            # Place order
            if price > history['close'].mean():
                await ctx.buy('AAPL', quantity=10)
    """

    def __init__(
        self,
        timestamp: datetime,
        bars: Dict[str, OHLCBar],
        broker: BaseBroker,
        data_loader: OHLCDataLoader
    ):
        self._timestamp = timestamp
        self._bars = bars
        self._broker = broker
        self._data_loader = data_loader
        self._cache: Dict[str, Any] = {}

    # Time Access

    @property
    def timestamp(self) -> datetime:
        """Current bar timestamp"""
        return self._timestamp

    @property
    def date(self) -> datetime:
        """Current date (alias for timestamp)"""
        return self._timestamp

    # Current Bar Data

    def bar(self, symbol: str) -> Optional[OHLCBar]:
        """Get current bar for symbol"""
        return self._bars.get(symbol)

    def open(self, symbol: str) -> Optional[float]:
        """Get current open price"""
        bar = self._bars.get(symbol)
        return bar.open if bar else None

    def high(self, symbol: str) -> Optional[float]:
        """Get current high price"""
        bar = self._bars.get(symbol)
        return bar.high if bar else None

    def low(self, symbol: str) -> Optional[float]:
        """Get current low price"""
        bar = self._bars.get(symbol)
        return bar.low if bar else None

    def close(self, symbol: str) -> Optional[float]:
        """Get current close price"""
        bar = self._bars.get(symbol)
        return bar.close if bar else None
def volume(self, symbol: str) -> Optional[int]:
    """Get current volume"""
    bar = self._bars.get(symbol)
    return bar.volume if bar else None

# Historical Data Access

async def history(
    self,
    symbol: str,
    bars: int = 100,
    timeframe: Optional[Timeframe] = None
) -> 'HistoricalData':
    """
    Get historical data for a symbol.

    Args:
        symbol: Trading symbol
        bars: Number of bars to fetch
        timeframe: Bar timeframe (defaults to context's timeframe)

    Returns:
        HistoricalData object with OHLCV arrays
    """
    cache_key = f"history_{symbol}_{bars}_{timeframe}"

    if cache_key in self._cache:
        return self._cache[cache_key]

    # Calculate date range
    end_date = self._timestamp
    # Estimate start date (rough approximation)
    days_needed = bars * 2  # Buffer for weekends/holidays
    start_date = end_date - timedelta(days=days_needed)

    # Fetch data
    all_bars = []
    async for batch in self._data_loader.load_data(
        symbols=[symbol],
        start_date=start_date,
        end_date=end_date,
        timeframe=timeframe or Timeframe.MINUTE_1
    ):
        all_bars.extend(batch)

    # Take last N bars
    recent_bars = all_bars[-bars:] if len(all_bars) > bars else all_bars

    # Convert to HistoricalData
    hist_data = HistoricalData.from_bars(recent_bars)
    self._cache[cache_key] = hist_data

    return hist_data

    # Broker Operations

    async def buy(
        self,
        symbol: str,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None
    ) -> OrderResponse:
    """
    Place a buy order.

    Args:
        symbol: Trading symbol
        quantity: Number of shares
        order_type: Type of order
        limit_price: Limit price (for limit orders)
        stop_price: Stop price (for stop orders)

    Returns:
        Order response
    """
    order = OrderRequest(
        symbol=symbol,
        side=OrderSide.BUY,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        stop_price=stop_price
    )
        return await self._broker.submit_order(order)

    async def sell(
        self,
        symbol: str,
        quantity: float,
        order_type: OrderType = OrderType.MARKET,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None
    ) -> OrderResponse:
    """
    Place a sell order.

    Args:
        symbol: Trading symbol
        quantity: Number of shares
        order_type: Type of order
        limit_price: Limit price (for limit orders)
        stop_price: Stop price (for stop orders)

    Returns:
        Order response
    """
    order = OrderRequest(
        symbol=symbol,
        side=OrderSide.SELL,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        stop_price=stop_price
    )
        return await self._broker.submit_order(order)

    async def close_position(self, symbol: str) -> Optional[OrderResponse]:
    """
    Close position for a symbol.

    Args:
        symbol: Trading symbol

    Returns:
        Order response if position existed, None otherwise
    """
        return await self._broker.close_position(symbol)

    # Position & Account Access

    async def position(self, symbol: str) -> Optional[Position]:
        """Get current position for symbol"""
        return await self._broker.get_position(symbol)

    async def positions(self) -> List[Position]:
        """Get all current positions"""
        return await self._broker.get_all_positions()

    async def account(self) -> Account:
        """Get account information"""
        return await self._broker.get_account()

    async def cash(self) -> float:
        """Get available cash"""
        account = await self.account()
        return account.cash

    async def portfolio_value(self) -> float:
        """Get total portfolio value"""
        account = await self.account()
        return account.portfolio_value

    # Utility Methods

    def has_position(self, symbol: str) -> bool:
        """Check if we have a position (synchronous check from cache)"""
        # This would need to be populated by the engine
        return symbol in self._bars  # Simplified

    def symbols(self) -> List[str]:
        """Get list of symbols in current context"""
        return list(self._bars.keys())


class HistoricalData:
    """
    Container for historical OHLCV data.

    Provides array access to historical prices and volumes.
    """

    def __init__(
        self,
        timestamps: List[datetime],
        opens: List[float],
        highs: List[float],
        lows: List[float],
        closes: List[float],
        volumes: List[int]
    ):
        self.timestamps = timestamps
        self.opens = opens
        self.highs = highs
        self.lows = lows
        self.closes = closes
        self.volumes = volumes

    @classmethod
    def from_bars(cls, bars: List[OHLCBar]) -> 'HistoricalData':
        """Create from list of OHLC bars"""
        return cls(
            timestamps=[b.timestamp for b in bars],
            opens=[b.open for b in bars],
            highs=[b.high for b in bars],
            lows=[b.low for b in bars],
            closes=[b.close for b in bars],
            volumes=[b.volume for b in bars]
        )

    def __len__(self) -> int:
        return len(self.timestamps)

    def __getitem__(self, key: str) -> List[float]:
        """Array access: data['close'], data['high'], etc."""
        if key == 'open':
            return self.opens
        elif key == 'high':
            return self.highs
        elif key == 'low':
            return self.lows
        elif key == 'close':
            return self.closes
        elif key == 'volume':
            return self.volumes
        else:
            raise KeyError(f"Unknown field: {key}")


### Context Extensions

The context can be extended with additional functionality through mixins or subclasses:

```python
class IndicatorMixin:
    """Mixin providing technical indicators"""

    async def sma(self, symbol: str, period: int = 20) -> float:
        """Calculate Simple Moving Average"""
        hist = await self.history(symbol, bars=period)
        return sum(hist.closes) / len(hist.closes)

    async def ema(self, symbol: str, period: int = 20) -> float:
        """Calculate Exponential Moving Average"""
        hist = await self.history(symbol, bars=period * 2)
        # EMA calculation
        multiplier = 2 / (period + 1)
        ema = hist.closes[0]
        for price in hist.closes[1:]:
            ema = (price - ema) * multiplier + ema
        return ema

    async def rsi(self, symbol: str, period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        hist = await self.history(symbol, bars=period + 1)

        gains = []
        losses = []

        for i in range(1, len(hist.closes)):
            change = hist.closes[i] - hist.closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        if avg_loss == 0:
            return 100

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi


class EnhancedContext(StrategyContext, IndicatorMixin):
    """Enhanced context with indicators"""
    pass
```

---

## Integration & Data Flow

### System Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Strategy Framework                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐                                           │
│  │   Strategy   │  (Natural Language → Python)              │
│  │   Generator  │                                           │
│  └──────┬───────┘                                           │
│         │                                                    │
│         ▼                                                    │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │   Strategy   │─────▶│   Context    │                    │
│  │   Executor   │      │   Manager    │                    │
│  └──────┬───────┘      └──────┬───────┘                    │
│         │                      │                            │
│         ▼                      ▼                            │
│  ┌──────────────┐      ┌──────────────┐                    │
│  │ Mode Switch  │      │ Data Loader  │                    │
│  │             │      └──────┬───────┘                    │
│  │  Live  Back │              │                            │
│  └──┬────┬─────┘              │                            │
│     │    │                    │                            │
│     │    │                    ▼                            │
│     │    │             ┌──────────────┐                    │
│     │    │             │  PostgreSQL  │                    │
│     │    │             │   (OHLC)     │                    │
│     │    │             └──────────────┘                    │
│     │    │                                                  │
│     │    └──────────────┐                                  │
│     │                   │                                  │
│     ▼                   ▼                                  │
│  ┌──────────────┐   ┌──────────────┐                      │
│  │ Live Broker  │   │  Simulated   │                      │
│  │  (Alpaca)    │   │   Broker     │                      │
│  └──────────────┘   └──────────────┘                      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow: Live Trading

```
1. Market Data Event
│
├─▶ Broker receives real-time bar
│
├─▶ Context updated with current bar
│
├─▶ Strategy executor calls strategy function
│
├─▶ Strategy analyzes context.close(), context.history()
│
├─▶ Strategy places order via context.buy()
│
├─▶ Context delegates to broker
│
└─▶ Broker submits order to Alpaca API
```

### Data Flow: Backtesting

```
1. Backtest Start
│
├─▶ Data loader fetches historical bars from PostgreSQL
│
├─▶ For each timestamp:
│   │
│   ├─▶ Update simulated broker prices
│   │
│   ├─▶ Create context with current bars
│   │
│   ├─▶ Execute strategy function
│   │
│   ├─▶ Strategy places order
│   │
│   ├─▶ Simulated broker fills immediately with slippage
│   │
│   └─▶ Record equity and trades
│
└─▶ Calculate metrics and return results
```

### Strategy Execution Interface

```python
from typing import Protocol


class StrategyExecutor:
    """
    Executes trading strategies in both live and backtest modes.

    Handles mode switching and context creation.
    """

    def __init__(self, strategy_code: str):
        """
        Initialize with generated strategy code.

        Args:
            strategy_code: Python code for strategy
        """
        self.strategy_code = strategy_code
        self.strategy_func = self._compile_strategy(strategy_code)

    def _compile_strategy(self, code: str) -> Callable:
        """Compile strategy code into callable function"""
        namespace = {}
        exec(code, namespace)
        return namespace['strategy']  # Assumes strategy function named 'strategy'

    async def run_live(
        self,
        broker: BaseBroker,
        symbols: List[str],
        timeframe: Timeframe
    ) -> None:
        """
        Run strategy in live trading mode.

        Args:
            broker: Live broker instance
            symbols: Symbols to trade
            timeframe: Bar timeframe
        """
        # Subscribe to real-time data
        # Execute strategy on each bar
        # Continues indefinitely
        pass

    async def run_backtest(
        self,
        config: BacktestConfig,
        data_loader: OHLCDataLoader
    ) -> BacktestResult:
        """
        Run strategy in backtest mode.

        Args:
            config: Backtest configuration
            data_loader: Historical data loader

        Returns:
            Backtest results with metrics
        """
        engine = BacktestEngine(config, data_loader, self.strategy_func)
        return await engine.run()
```

---

## Extension Points

### Adding New Brokers

1. **Create broker class** inheriting from [`BaseBroker`](src/engine/brokers/base.py:1)
2. **Implement all abstract methods** (connect, submit_order, etc.)
3. **Map broker-specific data** to universal models
4. **Register with factory**: `BrokerFactory.register('broker_name', BrokerClass)`

```python
class InteractiveBrokersBroker(BaseBroker):
    """Interactive Brokers implementation"""

    async def connect(self) -> None:
        # IB-specific connection logic
        pass

    async def submit_order(self, order: OrderRequest) -> OrderResponse:
        # Map to IB order format
        # Submit via IB API
        # Map response back
        pass

    # ... implement other methods
```

### Adding New Indicators

Extend the context with indicator mixins:

```python
class CustomIndicatorMixin:
    """Custom technical indicators"""

    async def vwap(self, symbol: str, bars: int = 20) -> float:
        """Volume Weighted Average Price"""
        hist = await self.history(symbol, bars=bars)

        typical_prices = [
            (h + l + c) / 3
            for h, l, c in zip(hist.highs, hist.lows, hist.closes)
        ]

        total_volume = sum(hist.volumes)
        vwap = sum(
            tp * v for tp, v in zip(typical_prices, hist.volumes)
        ) / total_volume

        return vwap


class CustomContext(StrategyContext, IndicatorMixin, CustomIndicatorMixin):
    """Context with all indicators"""
    pass
```

### Adding New Data Sources

Implement the data loader interface for alternative sources:

```python
class AlternativeDataLoader(OHLCDataLoader):
    """Load data from alternative source (API, files, etc.)"""

    async def load_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: Timeframe,
        batch_size: int = 10000
    ) -> AsyncIterator[List[OHLCBar]]:
        """Load from alternative source"""
        # Fetch from API or files
        # Yield batches in chronological order
        pass
```

### Adding New Order Types

Extend [`OrderType`](src/engine/brokers/base.py:1) enum and handle in broker implementations:

```python
class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    # New types:
    ICEBERG = "iceberg"
    BRACKET = "bracket"
    OCO = "oco"  # One-Cancels-Other
```

---

## Error Handling Strategy

### Error Hierarchy

```
BrokerError (base)
├── AuthenticationError
├── ConnectionError
├── OrderRejectedError
├── InsufficientFundsError
├── RateLimitError
├── SymbolNotFoundError
└── DataNotAvailableError
```

### Retry Strategy

```python
from tenacity import retry, stop_after_attempt, wait_exponential


class ResilientBroker:
    """Wrapper adding retry logic to broker operations"""

    def __init__(self, broker: BaseBroker):
        self.broker = broker

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=lambda e: isinstance(e, RateLimitError)
    )
    async def submit_order(self, order: OrderRequest) -> OrderResponse:
        """Submit order with automatic retry on rate limit"""
        return await self.broker.submit_order(order)
```

### Error Recovery

```python
class ErrorRecoveryManager:
    """Manages error recovery for strategy execution"""

    async def handle_broker_error(
        self,
        error: BrokerError,
        context: StrategyContext
    ) -> bool:
        """
        Handle broker error and decide if strategy should continue.

        Returns:
            True if strategy can continue, False if should stop
        """
        if isinstance(error, RateLimitError):
            # Wait and retry
            await asyncio.sleep(error.retry_after or 60)
            return True

        elif isinstance(error, InsufficientFundsError):
            # Log and continue (don't crash strategy)
            logger.warning(f"Insufficient funds: {error}")
            return True

        elif isinstance(error, AuthenticationError):
            # Critical error - stop strategy
            logger.error(f"Authentication failed: {error}")
            return False

        else:
            # Unknown error - log and continue cautiously
            logger.error(f"Broker error: {error}")
            return True
```

---

## Performance Considerations

### Database Query Optimization

```python
# Use indexes on OHLC table
CREATE INDEX idx_ohlc_symbol_timestamp
ON ohlc_data (symbol, timestamp DESC);

CREATE INDEX idx_ohlc_timeframe
ON ohlc_data (timeframe, timestamp DESC);

# Partition by date for large datasets
CREATE TABLE ohlc_data_2024 PARTITION OF ohlc_data
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Context Caching

The context object caches historical data requests to avoid redundant database queries:

```python
# First call - fetches from database
history = await context.history('AAPL', bars=100)

# Subsequent calls - returns cached data
history2 = await context.history('AAPL', bars=100)  # No DB query
```

### Batch Processing in Backtests

```python
# Load data in large batches for memory efficiency
BATCH_SIZE = 10000  # bars per batch

# Process multiple symbols simultaneously
async def process_batch(bars: List[OHLCBar]):
    # Group by timestamp
    # Execute strategy once per timestamp
    # Update all positions atomically
```

### Memory Management

```python
# For long backtests, periodically flush old data
class MemoryEfficientBacktestEngine(BacktestEngine):
    """Backtest engine with memory management"""

    def __init__(self, *args, max_cache_size: int = 1000, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_cache_size = max_cache_size

    async def _process_bars(self, timestamp: datetime, bars: List[OHLCBar]):
        await super()._process_bars(timestamp, bars)

        # Clear context cache if too large
        if len(self.context._cache) > self.max_cache_size:
            self.context._cache.clear()
```

### Async Execution

```python
# Execute multiple strategies concurrently
async def run_multiple_strategies(strategies: List[StrategyExecutor]):
    """Run multiple strategies in parallel"""
    tasks = [
        strategy.run_backtest(config, data_loader)
        for strategy in strategies
    ]
    results = await asyncio.gather(*tasks)
    return results
```

---

## Summary

This architecture provides a robust foundation for a trading strategy framework with the following key characteristics:

### **Module 1: Broker System**

- **Universal Interface**: [`BaseBroker`](src/engine/brokers/base.py:1) abstract class with standardized models
- **Lifecycle Management**: Async context managers for resource handling
- **Error Handling**: Comprehensive error hierarchy with retry mechanisms
- **Rate Limiting**: Token bucket algorithm for API compliance
- **Extensibility**: Factory pattern for easy broker addition

### **Module 2: Backtesting Engine**

- **Event-Driven**: Iterates chronologically through historical data
- **Realistic Simulation**: Configurable slippage and commission
- **Portfolio Tracking**: Maintains positions, equity curve, trade history
- **Performance Metrics**: Sharpe ratio, drawdown, win rate calculations
- **Memory Efficient**: Batch processing with configurable cache limits

### **Module 3: Context Object**

- **Unified Interface**: Same API for live trading and backtesting
- **Rich Data Access**: Current bar, historical data, indicators
- **Type-Safe**: Full type hints for IDE support
- **Extensible**: Mixin pattern for custom indicators
- **Performance**: Intelligent caching of historical queries

### **Design Patterns Used**

1. **Abstract Factory**: Broker creation
2. **Strategy**: Execution mode selection
3. **Observer**: Market data events
4. **Template Method**: Backtest execution flow
5. **Decorator**: Error recovery and retries
6. **Mixin**: Context extensions

### **Key Integration Points**

- Context bridges strategies and brokers
- Data loader provides unified access to OHLC data
- Simulated broker implements same interface as live brokers
- Factory pattern enables runtime broker selection

This architecture supports the full lifecycle from natural language strategy description through code generation, backtesting, and live execution while maintaining clean separation of concerns and extensibility for future enhancements.
````
