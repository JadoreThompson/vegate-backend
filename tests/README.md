# Backtest Engine Tests

Comprehensive test suite for the backtest engine, covering metrics calculation, broker simulation, and end-to-end backtesting scenarios.

## Test Structure

```
tests/
├── conftest.py              # Test configuration and fixtures
├── fixtures.py              # Reusable test data and mock strategies
├── test_metrics.py          # Tests for performance metrics
├── test_backtest_broker.py  # Tests for simulated broker
├── test_backtest_engine.py  # Tests for backtest engine
└── README.md               # This file
```

## Running Tests

### Run all tests

```bash
pytest tests/
```

### Run specific test file

```bash
pytest tests/test_metrics.py
pytest tests/test_backtest_broker.py
pytest tests/test_backtest_engine.py
```

### Run with coverage

```bash
pytest tests/ --cov=src/engine/backtesting --cov-report=html
```

### Run with verbose output

```bash
pytest tests/ -v
```

## Test Coverage

### test_metrics.py (24 tests)

Tests for performance metrics calculation:

- **Sharpe Ratio**: Positive/negative/volatile/flat returns, edge cases
- **Max Drawdown**: Single/multiple peaks, with/without cash curve
- **Total Return**: Profit/loss scenarios, edge cases

### test_backtest_broker.py (30 tests)

Tests for simulated broker functionality:

- **Market Orders**: Buy/sell execution, insufficient funds/position
- **Limit Orders**: Pending orders, fill conditions
- **Stop Orders**: Pending orders, trigger conditions
- **Order Management**: Cancel, get order, open orders
- **Account Management**: Balance tracking, equity calculation

### test_backtest_engine.py (20 tests)

Tests for end-to-end backtesting:

- **Engine Initialization**: Config validation, component setup
- **Strategy Execution**: Various trading strategies
- **Result Calculation**: Metrics, equity curves, order tracking
- **Edge Cases**: Empty data, single candle, no trades

### fixtures.py

Reusable test components:

- **OHLCV Generators**: Uptrend, downtrend, volatile, flat
- **Mock Strategies**: Buy once, buy & hold, limit orders, stop orders
- **Helper Functions**: Data creation utilities

## Environment Configuration

Tests use `.env.test` for configuration. All config variables from `config.py` are loaded with test values automatically via the `load_test_env` fixture in `conftest.py`.

## Key Test Patterns

### Testing Metrics

```python
def test_calculate_sharpe_ratio_with_positive_returns():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 101000),
        ...
    ]
    sharpe = calculate_sharpe_ratio(equity_curve)
    assert sharpe > 0
```

### Testing Broker Orders

```python
def test_submit_market_buy_order(broker, sample_candle):
    broker._current_candle = sample_candle
    order = OrderRequest(...)
    response = broker.submit_order(order)
    assert response.status == OrderStatus.FILLED
```

### Testing Backtest Engine

```python
def test_backtest_engine_with_strategy(backtest_config, mock_data):
    strategy = SimpleStrategy()
    engine = BacktestEngine(strategy, backtest_config)
    with patch.object(engine._broker, 'yield_historic_ohlcv', return_value=iter(mock_data)):
        result = engine.run()
    assert isinstance(result, SpotBacktestResult)
```

## Notes

- Tests use mocking to avoid database dependencies
- All monetary values are tested with appropriate precision
- Edge cases and error conditions are thoroughly covered
- Tests are isolated and can run in any order
