# Test Suite

Comprehensive test suite covering unit tests, integration tests, and end-to-end testing.

## Test Structure

```
tests/
├── conftest.py                      # Test configuration and fixtures
├── fixtures.py                      # Reusable test data and mock strategies
├── test_metrics.py                  # Tests for performance metrics
├── test_backtest_broker.py          # Tests for simulated broker
├── test_backtest_engine.py          # Tests for backtest engine
├── test_alpaca_broker.py            # Unit tests for Alpaca broker (with mocks)
├── integration/                     # Integration tests (real API calls)
│   ├── __init__.py
│   ├── conftest.py
│   ├── README.md
│   └── test_alpaca_broker_integration.py
└── README.md                        # This file
```

## Running Tests

### Run all tests (unit + integration)

```bash
pytest tests/ -v
```

### Run only unit tests (skip integration)

```bash
pytest -m "not integration" -v
```

### Run only integration tests

```bash
pytest -m integration -v
# OR
pytest tests/integration/ -v
```

### Run specific test file

```bash
pytest tests/test_metrics.py -v
pytest tests/test_backtest_broker.py -v
pytest tests/test_alpaca_broker.py -v
pytest tests/integration/test_alpaca_broker_integration.py -v
```

### Run with coverage

```bash
pytest tests/ --cov=src/engine --cov-report=html -m "not integration"
```

### Run with verbose output

```bash
pytest tests/ -v -s
```

## Test Coverage

### Unit Tests

#### test_metrics.py (24 tests)

Tests for performance metrics calculation:

- **Sharpe Ratio**: Positive/negative/volatile/flat returns, edge cases
- **Max Drawdown**: Single/multiple peaks, with/without cash curve
- **Total Return**: Profit/loss scenarios, edge cases

#### test_backtest_broker.py (30 tests)

Tests for simulated broker functionality:

- **Market Orders**: Buy/sell execution, insufficient funds/position
- **Limit Orders**: Pending orders, fill conditions
- **Stop Orders**: Pending orders, trigger conditions
- **Order Management**: Cancel, get order, open orders
- **Account Management**: Balance tracking, equity calculation

#### test_backtest_engine.py (20 tests)

Tests for end-to-end backtesting:

- **Engine Initialization**: Config validation, component setup
- **Strategy Execution**: Various trading strategies
- **Result Calculation**: Metrics, equity curves, order tracking
- **Edge Cases**: Empty data, single candle, no trades

#### test_alpaca_broker.py (40 tests)

Unit tests for Alpaca broker with mocked dependencies:

- **Connection**: Success, authentication errors, connection errors
- **Order Management**: Submit, cancel, retrieve orders
- **Account Info**: Get account details
- **Data Conversion**: Order type/status/time-in-force conversions
- **Async Support**: Disconnect async, event loop handling

### Integration Tests

#### test_alpaca_broker_integration.py (15+ tests)

Integration tests using real Alpaca API (paper trading):

- **Real Connection**: Connect/disconnect with actual API
- **Live Data**: Fetch real historical OHLCV data
- **Account Info**: Retrieve real account information
- **Rate Limiting**: Test actual API rate limit handling
- **Error Scenarios**: Test with invalid credentials
- **Concurrency**: Test concurrent API operations

See [integration/README.md](integration/README.md) for detailed integration test documentation.

### fixtures.py

Reusable test components:

- **OHLCV Generators**: Uptrend, downtrend, volatile, flat
- **Mock Strategies**: Buy once, buy & hold, limit orders, stop orders
- **Helper Functions**: Data creation utilities

## Environment Configuration

### Unit Tests

Tests use `.env.test` for configuration. All config variables from `config.py` are loaded with test values automatically via the `load_test_env` fixture in `conftest.py`.

### Integration Tests

Integration tests require valid Alpaca paper trading API credentials in `.env.test`:

```env
ALPACA_API_KEY=your_paper_trading_key
ALPACA_SECRET_KEY=your_paper_trading_secret
```

**Important**: Always use paper trading credentials for integration tests, never live trading credentials.

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

### General

- Tests use mocking to avoid database dependencies (unit tests)
- All monetary values are tested with appropriate precision
- Edge cases and error conditions are thoroughly covered
- Tests are isolated and can run in any order

### Unit Tests

- Fast execution (< 5 seconds for full suite)
- No external dependencies required
- Can run offline
- Safe to run in CI on every commit

### Integration Tests

- Slower execution (30-60 seconds)
- Require internet connection
- Require valid API credentials
- Respect API rate limits
- Should run separately from unit tests
- Best suited for scheduled CI runs

## CI/CD Recommendations

```yaml
# Run unit tests on every commit
unit-tests:
  run: pytest -m "not integration" -v

# Run integration tests on schedule or manual trigger
integration-tests:
  schedule: "0 */6 * * *" # Every 6 hours
  run: pytest -m integration -v
```

## Async Testing

Tests involving async operations use `pytest-asyncio` with session-scoped event loops:

```python
@pytest.mark.asyncio(loop_scope="session")
async def test_async_operation():
    result = await some_async_function()
    assert result is not None
```
