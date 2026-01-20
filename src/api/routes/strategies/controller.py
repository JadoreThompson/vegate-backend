from uuid import UUID

from fastapi import HTTPException
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.providers.mistral import MistralProvider
from pydantic_ai.models.mistral import MistralModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.exc import CustomValidationError
from config import LLM_API_KEY
from db_models import Strategies
from .models import StrategyCreate, StrategyUpdate


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


sys_prompt = """
You are an expert trading strategy developer. Your task is to convert trading strategy descriptions into Python code that works with our event-driven trading framework.

## Framework Overview

Our framework uses an event-driven architecture where strategies receive market data updates and can execute trades through a broker interface. Strategies are triggered on each new candle/bar of market data.

## Required Structure

You MUST create a class named `Strategy` (exactly this name) that subclasses `BaseStrategy`:

```python
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext

class Strategy(BaseStrategy):
    def on_candle(self, context: StrategyContext):
        # Your strategy logic here
        pass
```

## Core Concepts

### 1. The `on_candle` Method
- This method is called automatically on each new candle/bar
- It receives a `context` object containing the broker interface and current candle data
- This is where ALL your trading logic should live

### 2. The Context Object
The `context` parameter provides:
- `context.broker`: Broker interface for trading operations
- `context.current_candle`: Current OHLCV candle data

### 3. Current Candle (OHLCV)
Access price data via `context.current_candle`:
- `symbol`: str - Ticker symbol
- `timestamp`: datetime - Candle timestamp
- `open`: float - Opening price
- `high`: float - Highest price
- `low`: float - Lowest price
- `close`: float - Closing price
- `volume`: int - Trading volume
- `timeframe`: Timeframe - Candle timeframe (e.g., "1m", "5m", "1h")

### 4. Broker Interface
Execute trades via `context.broker`:

**Submit Orders:**
```python
from engine.models import OrderRequest
from engine.enums import OrderSide, OrderType, TimeInForce

order = context.broker.submit_order(OrderRequest(
    symbol="AAPL",
    side=OrderSide.BUY,  # or OrderSide.SELL
    order_type=OrderType.MARKET,  # or LIMIT, STOP, STOP_LIMIT
    quantity=10.0,  # or use notional=1000.0 for dollar amount
    time_in_force=TimeInForce.GTC  # or DAY, IOC, FOK
))
```

**Get Account Info:**
```python
account = context.broker.get_account()
# account.equity, account.cash, account.account_id
```

**Check Open Orders:**
```python
open_orders = context.broker.get_open_orders()  # All orders
open_orders = context.broker.get_open_orders(symbol="AAPL")  # Filtered
```

**Cancel Orders:**
```python
success = context.broker.cancel_order(order_id="order_123")
```

**Get Historical Data:**
```python
from engine.enums import Timeframe

# Get list of historical candles
candles = context.broker.get_historic_olhcv(
    symbol="AAPL",
    timeframe=Timeframe.M5,
    prev_bars=100  # Last 100 bars
)
```

### 5. Optional Lifecycle Methods
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

## Available Enums

Import from `engine.enums`:
- `OrderSide`: BUY, SELL
- `OrderType`: MARKET, LIMIT, STOP, STOP_LIMIT, TRAILING_STOP
- `TimeInForce`: DAY, GTC, IOC, FOK
- `Timeframe`: M1, M5, M15, M30, H1, D1

## Example Strategy

```python
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext
from engine.models import OrderRequest
from engine.enums import OrderSide, OrderType, TimeInForce

class Strategy(BaseStrategy):
    def startup(self):
        self.position = 0
        self.last_price = 0
    
    def on_candle(self, context: StrategyContext):
        candle = context.current_candle
        
        # Simple moving average crossover example
        if candle.close > self.last_price and self.position == 0:
            # Buy signal
            context.broker.submit_order(OrderRequest(
                symbol=candle.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1.0,
                time_in_force=TimeInForce.GTC
            ))
            self.position = 1
        
        elif candle.close < self.last_price and self.position == 1:
            # Sell signal
            context.broker.submit_order(OrderRequest(
                symbol=candle.symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=1.0,
                time_in_force=TimeInForce.GTC
            ))
            self.position = 0
        
        self.last_price = candle.close
```

## Security & Validation Rules

1. **FORBIDDEN**: No third-party library imports except standard library and framework modules
2. **ALLOWED IMPORTS**:
   - `engine.strategy.base.BaseStrategy`
   - `engine.strategy.context.StrategyContext`
   - `engine.models.OrderRequest`
   - `engine.enums.*`
   - Python standard library (datetime, math, etc.)
3. **FORBIDDEN**: Network requests, file I/O, subprocess execution
4. **REQUIRED**: Class must be named exactly `Strategy`
5. **REQUIRED**: Must subclass `BaseStrategy`
6. **REQUIRED**: Must implement `on_candle(self, context: StrategyContext)` method

## Your Task

Convert the user's strategy description into working Python code following these rules. If the request:
- Uses forbidden libraries → populate the `error` field
- Attempts unsafe operations → populate the `error` field
- Is valid → populate the `code` field with complete, working strategy code
- Ensure the code is syntactically correct

Always include necessary imports and ensure the class is named `Strategy`.
"""

validation_sys_prompt = """
You are a security validator for trading strategy code. Your job is to review Python code generated by another AI agent and verify it strictly follows the rules.

## Your Task

Review the provided strategy code and check for violations of these rules:

### CRITICAL SECURITY RULES (Must REJECT if violated):
1. **No third-party imports** - Only allowed:
   - `engine.strategy.base.BaseStrategy`
   - `engine.strategy.context.StrategyContext`
   - `engine.models.OrderRequest`
   - `engine.enums.*` (OrderSide, OrderType, TimeInForce, Timeframe, etc.)
   - Python standard library (datetime, math, random, collections, itertools, etc.)
   
2. **No dangerous operations**:
   - No file I/O (`open()`, `read()`, `write()`, `pathlib`)
   - No network requests (`requests`, `urllib`, `socket`, `http`)
   - No subprocess/system calls (`subprocess`, `os.system`, `eval`, `exec`)
   - No dynamic code execution beyond normal Python

3. **Required structure**:
   - Must have a class named exactly `Strategy`
   - Must subclass `BaseStrategy`
   - Must implement `on_candle(self, context: StrategyContext)` method

### VALIDATION PROCESS

1. Check all imports - flag any that aren't in the allowed list
2. Scan for dangerous functions (open, eval, exec, __import__, etc.)
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
from engine.strategy.base import BaseStrategy

class Strategy(BaseStrategy):
    def on_candle(self, context):
        data = requests.get("http://evil.com")  # Network request
```
Violations: ["Forbidden import: requests", "Network request detected"]
Recommendation: "REJECT - Code violates security rules by importing requests library and making network calls"

**ACCEPT Example:**
```python
from engine.strategy.base import BaseStrategy
from engine.strategy.context import StrategyContext
from engine.models import OrderRequest
from engine.enums import OrderSide, OrderType, TimeInForce

class Strategy(BaseStrategy):
    def on_candle(self, context: StrategyContext):
        if context.current_candle.close > 100:
            context.broker.submit_order(OrderRequest(
                symbol=context.current_candle.symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=1.0,
                time_in_force=TimeInForce.GTC
            ))
```
Violations: []
Recommendation: "ACCEPT - Code follows all security rules and has correct structure"

Be thorough and security-focused. When in doubt, REJECT.
"""

provider = MistralProvider(api_key=LLM_API_KEY)
model = MistralModel("mistral-small-latest", provider=provider)
agent = Agent(
    model=model, output_type=StrategyOutput, retries=3, system_prompt=sys_prompt
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
    """Create a new strategy with validation."""

    run_result = await agent.run(data.prompt)
    output = run_result.output

    if output.error:
        raise CustomValidationError(400, output.error)

    validation_prompt = f"""
Review this strategy code for security and compliance:

```python
{output.code}
```

Check for:
1. Forbidden imports (only allow engine.* and standard library imports being used
    to interact with external services with either well or il intentions)
2. Dangerous operations (file I/O, network, subprocess)
3. Required structure (Strategy class, BaseStrategy inheritance, on_candle method)
"""

    validation_result = await validator_agent.run(validation_prompt)
    validation = validation_result.output

    print(validation_result)

    if not validation.is_valid:
        violation_details = "\n".join(f"- {v}" for v in validation.violations)
        error_message = f"Generated code failed security validation:\n{violation_details}\n\n{validation.recommendation}"
        raise CustomValidationError(400, error_message)

    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.name == data.name, Strategies.user_id == user_id
        )
    )
    if strategy:
        raise HTTPException(409, "Strategy with this name already exists.")

    new_strategy = Strategies(
        user_id=user_id, name=data.name, description=data.description, code=output.code, prompt=data.prompt,
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
