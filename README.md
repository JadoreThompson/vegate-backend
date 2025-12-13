# Vegate Backend - AI-Powered Algorithmic Trading Platform

**The backend engine powering automated trading strategies with AI-driven code generation.**

Vegate Backend is a Python application that processes natural language trading strategies, executes backtests against historical market data, and manages live deployments to broker accounts. Built with FastAPI and modern async Python, it provides a robust foundation for algorithmic trading at scale.

---

## 🌟 Key Features

### 🤖 AI Strategy Generation

Converts natural language trading descriptions into executable Python code using advanced language models:

- Integrates with Pydantic AI for strategy code generation
- Validates generated strategies against a base template
- Supports complex technical indicators and risk management rules
- Automatic error handling and edge case coverage

### 📊 High-Performance Backtesting Engine

Event-driven backtesting system with realistic market simulation:

- Tick-level historical data processing
- Accurate order execution modeling with slippage
- Support for multiple asset classes (stocks, crypto)
- Comprehensive performance metrics (Sharpe ratio, max drawdown, P&L)
- Efficient OHLCV data management with caching

### 🚀 Live Trading Deployment

Production-ready system for executing strategies in real markets:

- Asynchronous deployment runner with event-driven control
- Real-time order execution via broker APIs
- WebSocket-based market data streaming
- Graceful shutdown and error recovery
- Multi-deployment support with isolated contexts

### 🔌 Broker Integration Framework

Unified interface for multiple broker connections:

- **Alpaca Markets** - Full OAuth 2.0 integration with paper/live trading
- **Interactive Brokers** - Coming soon
- **IG Markets** - Coming soon
- Encrypted credential storage
- Rate limiting and request management
- Automatic token refresh

### 📈 Real-Time Market Data

Efficient data pipeline for live and historical market data:

- Redis-based caching layer
- Support for multiple timeframes (1min, 5min, 15min, 1hour, 1day)
- OHLCV data builder with tick aggregation
- Kafka integration for event streaming
- Historical data fetching with pagination

### 🔐 Security & Authentication

Enterprise-grade security implementation:

- JWT-based authentication with configurable expiry
- Argon2 password hashing
- AES-256 encryption for sensitive data (OAuth tokens, API keys)
- Rate limiting middleware
- CORS configuration for frontend integration

---

## 🛠️ Technology Stack

### Core Framework

- **Python 3.13** - Modern Python with latest performance improvements
- **FastAPI** - High-performance async web framework
- **Uvicorn** - Lightning-fast ASGI server
- **Pydantic** - Data validation and settings management
- **SQLAlchemy 2.0** - Modern async ORM with type hints

### Data Storage

- **PostgreSQL** - Primary relational database
- **Redis** - Caching and pub/sub for real-time events
- **Kafka** - Event streaming for market data (optional)

### Trading Infrastructure

- **Alpaca-py** - Official Alpaca Markets SDK
- **websockets** - WebSocket client for real-time data

### Development Tools

- **uv** - Ultra-fast Python package manager
- **Alembic** - Database migrations
- **pytest** - Testing framework
- **Click** - CLI framework for management commands

### Security & Services

- **cryptography** - Encryption and secure data handling
- **PyJWT** - JSON Web Token implementation
- **argon2-cffi** - Secure password hashing

---

## 🚀 Getting Started

### Prerequisites

- Python 3.13+
- PostgreSQL 16+
- Redis 7+
- uv

### Installation

1. **Clone the repository**

```bash
git clone https://github.com/JadoreThompson/vegate-backend.git
cd vegate-backend
```

3. **Configure environment variables**

```bash
cp .env.example .env
```

4. **Set up the database**

```bash
# Create database
create database if not exists vegate;

# Copy Alembic configuration
cp alembic.ini.example alembic.ini

# Run migrations
uv run vegate db migrate
```

5. **Start the API server**

```bash
uv run vegate backend run
```

The API will be available at `http://localhost:8000`

### Docker Deployment

Build and run with Docker Compose:

```bash
# Development environment
docker-compose -f docker/dev-compose.yaml up

# Production environment
docker-compose -f docker/prod-compose.yaml up -d
```

---

## 🔧 CLI Commands

The backend includes a comprehensive CLI for managing the platform:

### Backend Management

```bash
# Start all backend processes
uv run vegate backend run
```

### Database Operations

```bash
# Run migrations
uv run vegate db migrate

# Create a new migration
uv run vegate db revision -m "description"

# Rollback migration
uv run vegate db downgrade

# Reset database (caution: deletes all data)
uv run vegate db reset
```

### Backtesting

```bash
# Run a backtest from CLI
uv run vegate backtest run \
  --backtest-id <backtest-id>
```

### Live Deployment

```bash
# Deploy a strategy
uv run vegate deployment run --deployment-id <deployment-id>
```

### Data Pipeline

```bash
# Start market data listener
uv run vegate pipeline run --broker alpaca --symbol SPY --market stocks --timeframe 1m
```

---

### Key Components

#### Backtesting Engine

The backtesting engine simulates strategy execution against historical data:

```python
from engine.backtesting import BacktestEngine, BacktestConfig
from engine.enums import Timeframe
from datetime import date

config = BacktestConfig(
    symbol="SPY",
    start_date=date(2023, 1, 1),
    end_date=date(2023, 12, 31),
    starting_balance=100000.0,
    timeframe=Timeframe.DAY_1
)

engine = BacktestEngine(strategy=my_strategy, config=config)
result = engine.run()

print(f"Total Return: {result.total_return_pct:.2f}%")
print(f"Sharpe Ratio: {result.sharpe_ratio:.2f}")
print(f"Max Drawdown: {result.max_drawdown:.2f}%")
```

#### Strategy Base Class

All strategies inherit from [`BaseStrategy`](src/engine/strategy/base.py):

```python
from engine.strategy import BaseStrategy, StrategyContext

class MyStrategy(BaseStrategy):
    def on_candle(self, context: StrategyContext):
        """Called for each new candle"""
        candle = context.current_candle

        # Your trading logic here
        if should_buy(candle):
            context.broker.submit_order(OrderRequest(
                symbol=candle.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.GTC,
                qty=0.01,
            ))

    def cleanup(self):
        """Cleanup resources"""
        pass
```

#### Broker Interface

Unified broker interface in [`BaseBroker`](src/engine/brokers/base.py):

```python
from engine.brokers import AlpacaBroker

broker = AlpacaBroker(
    deployment_id="...",
    oauth_token="...",
    paper=True
)

# Get account info
account = broker.get_account()
print(f"Cash: ${account.cash}")

# Stream live data
async for candle in broker.yield_ohlcv_async("SPY", Timeframe.m1):
    print(f"Price: ${candle.close}")
```

---

## 🚧 Roadmap

- [ ] Additional broker integrations (Interactive Brokers, IG Markets)
- [ ] Options trading support
- [ ] Advanced order types (OCO, bracket orders)
- [ ] Support for more order operations
- [ ] Machine learning model integration
- [ ] Paper trading simulator improvements
- [ ] WebSocket API for real-time updates
- [ ] Advanced backtesting features (walk-forward optimization)

---

## 📄 License

This project is proprietary software. All rights reserved.
