from datetime import UTC, datetime
import logging
from uuid import UUID

from fastapi import HTTPException
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.providers.mistral import MistralProvider
from pydantic_ai.models.mistral import MistralModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config import LLM_API_KEY
from infra.db.models import Strategies, Backtests, OHLCs
from .models import StrategyCreate, StrategyUpdate, BacktestCreate
from enums import BacktestStatus, BrokerType
from api.backtest_queue import get_backtest_queue


logger = logging.getLogger("strategies.controller")


class StrategyOutput(BaseModel):
    error: str | None = Field(None, description="Description of the error")
    code: str | None = Field(
        None, description="The strategy class created from the provided prompt"
    )


class ValidationOutput(BaseModel):
    is_valid: bool = Field(description="Whether the code passes all validation checks")
    violations: list[str] = Field(
        default_factory=list, description="List of specific rule violations found"
    )
    recommendation: str | None = Field(
        None, description="Recommendation on whether to accept or reject the code"
    )


class CodeReviewOutput(BaseModel):
    is_valid: bool = Field(description="Whether the code is syntactically correct")
    errors: list[str] = Field(
        default_factory=list, description="List of syntax or logical errors found"
    )
    corrected_code: str | None = Field(
        None, description="Corrected code if errors were found"
    )


strategy_gen_sys_prompt = '''
You are an expert trading strategy developer. Your task is to convert trading strategy descriptions into Python code that works with our event-driven trading framework.

## Framework Overview

Our framework uses an event-driven architecture where strategies receive market data updates and can execute trades through a broker interface. Strategies are triggered on each new candle/bar of market data.

## Required Structure

You MUST create a class named `Strategy` (exactly this name) that subclasses `BaseStrategy`:

```python
from lib.strategy import BaseStrategy
from models import OHLC

class Strategy(BaseStrategy):
    def on_candle(self, candle: OHLC):
        # Your strategy logic here
        pass
```

## Core Concepts

### 1. The `on_candle` Method
- This method is called automatically on each new candle/bar
- It receives an OHLC candle object containing the current market data
- This is where ALL your trading logic should live

### 2. Current Candle (OHLC)
Access price data via the `candle` parameter:
- `symbol`: str - Ticker symbol
- `timestamp`: datetime - Candle timestamp
- `open`: float - Opening price
- `high`: float - Highest price
- `low`: float - Lowest price
- `close`: float - Closing price
- `volume`: float - Trading volume
- `timeframe`: str - Candle timeframe (e.g., "1m", "5m", "1h")

### 3. Broker Interface
Access the broker via `self.broker` (inherited from BaseStrategy):

**Submit Orders:**
```python
from models import OrderRequest
from enums import OrderType, OrderSide

order = self.broker.place_order(OrderRequest(
    symbol="AAPL",
    quantity=10.0,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY
))
```

Or if you want to submit orders based on notional value:
```python
order = self.broker.place_order(OrderRequest(
    symbol="AAPL",
    notional=1000.0,
    order_type=OrderType.MARKET,
    side=OrderSide.BUY
))
```


**Get Orders:**
```python
orders = self.broker.get_orders()
```

**Close Orders:**
```python
success = self.broker.close_order(order_id="order_123")
```

**Modify Orders:**
```python
success = self.broker.modify_order(order_id="order_123", limit_price=150.0)
```

### 4. Optional Lifecycle Methods
```python
def startup(self):
    # Called once when strategy initializes
    # Use for setup, loading indicators, etc.
    pass

def shutdown(self):
    # Called once when strategy stops
    # Use for cleanup, closing positions, etc.
    pass
```

Here is the code for the BaseStrategy class:

```python
import logging
from abc import ABC, abstractmethod

from lib.brokers import BaseBroker
from models import OHLC


class BaseStrategy(ABC):
    def __init__(self, name: str, broker: BaseBroker):
        """Initialize the strategy.

        Args:
            name: Name of the strategy (used as logger name)
            broker: Broker instance for placing orders
        """
        self._name = name
        self.broker = broker
        self.logger = logging.getLogger(name)

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def on_candle(self, candle: OHLC) -> None:
        """Called when a new candle is received.

        Args:
            candle: OHLC candle
        """
        pass

    def startup(self) -> None:
        """Called once at the start of backtesting. Override to initialize strategy state."""
        pass

    def shutdown(self) -> None:
        """Called once at the end of backtesting. Override to cleanup strategy state."""
        pass
```

Here is the code for the BaseBroker class defining the interface for all brokers:
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Generator

from models import Order, OrderRequest, OHLC
from enums import Timeframe


class BaseBroker(ABC):
    """Abstract base class for broker implementations."""

    def __init__(self):
        self.broker = None

    supports_async: bool = False

    @abstractmethod
    def place_order(self, order_request: OrderRequest) -> Order:
        """Place an order.

        Args:
            order_request: OrderRequest object

        Returns:
            Order object
        """
        pass

    @abstractmethod
    def modify_order(
        self,
        order_id: str,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        """Modify an existing order.

        Args:
            order_id: ID of order to modify
            limit_price: New limit price (optional)
            stop_price: New stop price (optional)

        Returns:
            Modified Order object
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled successfully, False otherwise
        """
        pass

    @abstractmethod
    def cancel_all_orders(self) -> bool:
        """Cancel all orders.

        Returns:
            True if all orders cancelled successfully, False otherwise
        """
        pass

    @abstractmethod
    def get_order(self, order_id: str) -> Order | None:
        """Get a specific order.

        Args:
            order_id: ID of order to retrieve

        Returns:
            Order object or None if not found
        """
        pass

    @abstractmethod
    def get_orders(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of Order objects
        """
        pass


If you are to define the __init__ method, you're to accept args and kwargs, pass the
args and kwargs to the super initialiser and then define what you need to define

## Available Imports

Only import from these packages:
- `lib.strategy.BaseStrategy` - Base strategy class
- `models.OHLC, OrderRequest` - Data models
- `enums.OrderType` - Order type enumeration
- Python standard library (datetime, math, collections, etc.)

## Example Strategy

```python
from lib.strategy import BaseStrategy
from models import OHLC, OrderRequest
from enums import OrderType

class Strategy(BaseStrategy):
    def startup(self):
        self.position = 0
        self.last_price = 0

    def on_candle(self, candle: OHLC):
        # Simple moving average crossover example
        if candle.close > self.last_price and self.position == 0:
            # Buy signal
            self.broker.place_order(OrderRequest(
                symbol=candle.symbol,
                quantity=1.0,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY
            ))
            self.position = 1

        elif candle.close < self.last_price and self.position == 1:
            # Sell signal
            self.broker.place_order(OrderRequest(
                symbol=candle.symbol,
                quantity=1.0,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL                
            ))
            self.position = 0

        self.last_price = candle.close
```

## Security & Validation Rules

1. **FORBIDDEN**: No third-party library imports except standard library and our framework modules
2. **ALLOWED IMPORTS**:
   - `lib.strategy.BaseStrategy`
   - `models.OHLC, OrderRequest`
   - `enums.OrderType`
   - Python standard library (datetime, math, etc.)
3. **FORBIDDEN**: Network requests, file I/O, subprocess execution, database access, OS commands
4. **REQUIRED**: Class must be named exactly `Strategy`
5. **REQUIRED**: Must subclass `BaseStrategy`
6. **REQUIRED**: Must implement `on_candle(self, candle: OHLC)` method

## Your Task

Convert the user's strategy description into working Python code following these rules. If the request:
- Uses forbidden libraries → populate the `error` field
- Attempts unsafe operations → populate the `error` field
- Is valid → populate the `code` field with complete, working strategy code
- Ensure the code is syntactically correct

Always include necessary imports and ensure the class is named `Strategy`.
'''

validation_sys_prompt = """
You are a security validator for trading strategy code. Your job is to review Python code generated by another AI agent and verify it strictly follows the rules.

## Your Task

Review the provided strategy code and check for violations of these rules:

### CRITICAL SECURITY RULES (Must REJECT if violated):
1. **No third-party imports** - Only allowed:
   - `lib.strategy.BaseStrategy`
   - `models.*`
   - `enums.*`
   - Python standard library (datetime, math, random, collections, itertools, etc.)

2. **No dangerous operations**:
   - No file I/O (`open()`, `read()`, `write()`, `pathlib`)
   - No network requests (`requests`, `urllib`, `socket`, `http`)
   - No subprocess/system calls (`subprocess`, `os.system`, `eval`, `exec`)
   - No database access (`sqlalchemy`, `psycopg2`, etc.)
   - No OS commands (`os.`, `shutil`)
   - No dynamic code execution beyond normal Python

3. **Required structure**:
   - Must have a class named exactly `Strategy`
   - Must subclass `BaseStrategy`
   - Must implement `on_candle(self, candle: OHLC)` method

### VALIDATION PROCESS

1. Check all imports - flag any that aren't in the allowed list
2. Scan for dangerous functions (open, eval, exec, __import__, os., subprocess, etc.)
3. Verify class structure (Strategy class exists, inherits BaseStrategy)
4. Verify on_candle method exists with correct signature
5. Look for suspicious patterns (obfuscation, encoding, reflection tricks)

### OUTPUT FORMAT

- `is_valid`: True only if ALL rules are followed
- `violations`: List each specific violation found (e.g., "Forbidden import: requests", "Missing on_candle method")
- `recommendation`: Clear statement to ACCEPT or REJECT with reasoning

### EXAMPLES

**REJECT Example:**
```python
import requests  # FORBIDDEN
from lib.strategy import BaseStrategy

class Strategy(BaseStrategy):
    def on_candle(self, candle):
        data = requests.get("http://evil.com")  # Network request
```
Violations: ["Forbidden import: requests", "Network request detected"]
Recommendation: "REJECT - Code violates security rules by importing requests library and making network calls"

**ACCEPT Example:**
```python
from lib.strategy import BaseStrategy
from models import OHLC, OrderRequest
from enums import OrderType

class Strategy(BaseStrategy):
    def on_candle(self, candle: OHLC):
        if candle.close > 100:
            self.broker.place_order(OrderRequest(
                symbol=candle.symbol,
                quantity=1.0,
                order_type=OrderType.MARKET,
                price=candle.close
            ))
```
Violations: []
Recommendation: "ACCEPT - Code follows all security rules and has correct structure"

Be thorough and security-focused. When in doubt, REJECT.
"""

code_review_sys_prompt = """
You are a Python code reviewer specializing in trading strategy code. Your task is to review generated strategy code for syntactic correctness and logical consistency.

## Your Task

Review the provided strategy code and check for:

1. **Syntax Errors**:
   - Invalid Python syntax
   - Missing colons, parentheses, brackets
   - Incorrect indentation
   - Invalid method signatures

2. **Logical Errors**:
   - Undefined variables or methods
   - Type mismatches
   - Missing required methods (on_candle)
   - Incorrect method signatures

3. **Import Issues**:
   - Missing imports for used classes/functions
   - Circular imports

## OUTPUT FORMAT

- `is_valid`: True only if code is syntactically correct and logically sound
- `errors`: List each specific error found (e.g., "Missing import for OrderRequest", "Syntax error on line 5")
- `corrected_code`: If errors exist, provide the corrected code. Otherwise, return None.

## EXAMPLES

**INVALID Example:**
```python
from lib.strategy import BaseStrategy

class Strategy(BaseStrategy):
    def on_candle(self, candle)  # Missing colon
        if candle.close > 100
            self.broker.place_order(...)  # Missing colon
```
Errors: ["Missing colon after method definition on line 4", "Missing colon after if statement on line 5"]

**VALID Example:**
```python
from lib.strategy import BaseStrategy
from models import OHLC, OrderRequest
from enums import OrderType

class Strategy(BaseStrategy):
    def on_candle(self, candle: OHLC):
        if candle.close > 100:
            self.broker.place_order(OrderRequest(
                symbol=candle.symbol,
                quantity=1.0,
                order_type=OrderType.MARKET,
                price=candle.close
            ))
```
Errors: []

Be thorough and strict about syntax. If you find errors, provide corrected code.
"""

provider = MistralProvider(api_key=LLM_API_KEY)
model = MistralModel("mistral-small-latest", provider=provider)
strategy_gen_agent = Agent(
    model=model,
    output_type=StrategyOutput,
    retries=3,
    system_prompt=strategy_gen_sys_prompt,
)
code_review_agent = Agent(
    model=model,
    output_type=CodeReviewOutput,
    retries=2,
    system_prompt=code_review_sys_prompt,
)
validator_agent = Agent(
    model=model,
    output_type=ValidationOutput,
    retries=2,
    system_prompt=validation_sys_prompt,
)


async def create_strategy(
    user_id: UUID, data: StrategyCreate, db_sess: AsyncSession
) -> Strategies:
    """Create a new strategy with code review and validation."""

    # Step 1: Generate strategy code
    run_result = await strategy_gen_agent.run(data.prompt)
    output = run_result.output

    if output.error:
        raise HTTPException(status_code=400, detail=output.error)

    strategy_code = output.code

    # Step 2: Code review with retry logic (max 3 attempts)
    max_review_attempts = 3
    for attempt in range(max_review_attempts):
        code_review_prompt = f"""
Review this strategy code for syntax and logical correctness:

```python
{strategy_code}
```

Check for syntax errors, missing imports, and logical issues.
"""
        review_result = await code_review_agent.run(code_review_prompt)
        review = review_result.output

        if review.is_valid:
            break

        # If there are errors and we have corrected code, use it
        if review.corrected_code:
            strategy_code = review.corrected_code
        else:
            # If this is the last attempt and still invalid, raise error
            if attempt == max_review_attempts - 1:
                error_details = "\n".join(f"- {e}" for e in review.errors)
                raise HTTPException(
                    status_code=400,
                    detail=f"Code review failed after {max_review_attempts} attempts:\n{error_details}",
                )

    # Step 3: Security validation
    validation_prompt = f"""
Review this strategy code for security and compliance:

```python
{strategy_code}
```

Check for:
1. Forbidden imports (only allow lib.*, models.*, enums.* and standard library)
2. Dangerous operations (file I/O, network, subprocess, database access, OS commands)
3. Required structure (Strategy class, BaseStrategy inheritance, on_candle method)
"""

    validation_result = await validator_agent.run(validation_prompt)
    validation = validation_result.output

    if not validation.is_valid:
        violation_details = "\n".join(f"- {v}" for v in validation.violations)
        error_message = f"Generated code failed security validation:\n{violation_details}\n\n{validation.recommendation}"
        raise HTTPException(status_code=400, detail=error_message)

    # Step 4: Check if strategy already exists
    existing_strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.name == data.name, Strategies.user_id == user_id
        )
    )
    if existing_strategy:
        raise HTTPException(
            status_code=409, detail="Strategy with this name already exists."
        )

    # Step 5: Persist strategy to database (without committing transaction)
    new_strategy = Strategies(
        user_id=user_id,
        name=data.name,
        description=data.description,
        code=strategy_code,
        prompt=data.prompt,
    )
    db_sess.add(new_strategy)
    await db_sess.flush()
    await db_sess.refresh(new_strategy)
    return new_strategy


async def get_strategy(strategy_id: UUID, db_sess: AsyncSession) -> Strategies | None:
    """Get a strategy by ID."""
    return await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == strategy_id)
    )


async def list_strategies(
    user_id: UUID, db_sess: AsyncSession, offset: int = 0, limit: int = 100
) -> list[Strategies]:
    """List all strategies with pagination."""
    result = await db_sess.execute(
        select(Strategies)
        .where(Strategies.user_id == user_id)
        .offset(offset)
        .limit(limit)
        .order_by(Strategies.created_at.desc())
    )
    return list(result.scalars().all())


async def update_strategy(
    user_id: UUID,
    strategy_id: UUID,
    data: StrategyUpdate,
    db_sess: AsyncSession,
) -> Strategies | None:
    """Update a strategy."""
    strategy = await get_strategy(db_sess, strategy_id)
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Strategy not found")

    if data.name and data.name != strategy.name:
        existing = await db_sess.scalar(
            select(Strategies).where(
                Strategies.name == data.name,
                Strategies.user_id == user_id,
                Strategies.strategy_id != strategy_id,
            )
        )
        if existing:
            raise HTTPException(409, "Strategy with this name already exists")

    # Update only provided fields
    update_data = data.model_dump(exclude_unset=True)
    if update_data:
        await db_sess.execute(
            update(Strategies)
            .where(Strategies.strategy_id == strategy_id)
            .values(**update_data)
        )
        await db_sess.flush()
        await db_sess.refresh(strategy)

    return strategy


async def get_strategy_summary(
    user_id: UUID, strategy_id: UUID, db_sess: AsyncSession
) -> Strategies | None:
    """Get a strategy with its metrics for summary view."""
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id, Strategies.user_id == user_id
        )
    )
    return strategy


async def list_strategy_summaries(
    user_id: UUID, db_sess: AsyncSession, offset: int = 0, limit: int = 100
) -> list[Strategies]:
    """List all strategies with metrics for summary view."""
    result = await db_sess.execute(
        select(Strategies)
        .where(Strategies.user_id == user_id)
        .offset(offset)
        .limit(limit)
        .order_by(Strategies.created_at.desc())
    )
    return list(result.scalars().all())


async def create_backtest(
    user_id: UUID, strategy_id: UUID, data: BacktestCreate, db_sess: AsyncSession
) -> Backtests:
    """Create a new backtest for a strategy.

    Validates that:
    1. Strategy exists and belongs to user
    2. Data exists for the specified period, timeframe, symbol, and broker
    3. Creates backtest record and launches backtest runner
    """
    # Step 1: Verify strategy exists and belongs to user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id, Strategies.user_id == user_id
        )
    )
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")

    # Step 2: Validate broker type
    try:
        broker_type = BrokerType(data.broker)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid broker type. Allowed values: {', '.join([b.value for b in BrokerType])}",
        )

    # Step 3: Check if OHLC data exists for the specified period
    date_to_utc_timestamp = lambda d: datetime(
        d.year, d.month, d.day, tzinfo=UTC
    ).timestamp()
    ohlc_count = await db_sess.scalar(
        select(OHLCs).where(
            OHLCs.source == broker_type.value,
            OHLCs.symbol == data.symbol,
            OHLCs.timeframe == data.timeframe,
            OHLCs.timestamp >= int(date_to_utc_timestamp(data.start_date)),
            OHLCs.timestamp < int(date_to_utc_timestamp(data.end_date)),
        )
    )

    if not ohlc_count:
        raise HTTPException(
            status_code=400,
            detail=f"No OHLC data found for {data.symbol} on {data.broker} with timeframe {data.timeframe} for the specified date range",
        )

    # Step 4: Create backtest record
    new_backtest = Backtests(
        strategy_id=strategy_id,
        symbol=data.symbol,
        starting_balance=data.starting_balance,
        metrics=None,
        start_date=data.start_date,
        end_date=data.end_date,
        timeframe=data.timeframe,
        status=BacktestStatus.PENDING.value,
        server_data={"broker": broker_type.value},
    )
    db_sess.add(new_backtest)
    await db_sess.flush()
    await db_sess.refresh(new_backtest)

    # Step 5: Push backtest_id to queue for processing
    backtest_queue = get_backtest_queue()
    if backtest_queue is not None:
        backtest_queue.put(new_backtest.backtest_id)
    else:
        logger.info("Backtest queue not set.")
    return new_backtest
