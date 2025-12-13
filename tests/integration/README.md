# Integration Tests

This directory contains integration tests that interact with real external services (Alpaca API).

## Overview

Integration tests verify that the application works correctly with actual API responses, catching issues that unit tests with mocks might miss. These tests:

- Connect to real Alpaca paper trading API
- Fetch real market data
- Test actual API rate limiting behavior
- Verify error handling with real API responses
- Ensure compatibility with current API versions

## Requirements

### API Credentials

Integration tests require valid Alpaca paper trading credentials in `.env.test`:

```env
ALPACA_API_KEY=your_paper_trading_key
ALPACA_SECRET_KEY=your_paper_trading_secret
```

**Important**: Always use paper trading credentials, never live trading credentials.

### Other Requirements

- Internet connection
- Alpaca API availability
- pytest-asyncio installed

## Running Integration Tests

### Run all integration tests:

```bash
pytest tests/integration/ -v
```

### Run with integration marker:

```bash
pytest -m integration -v
```

### Run specific integration test file:

```bash
pytest tests/integration/test_alpaca_broker_integration.py -v
```

### Run specific test:

```bash
pytest tests/integration/test_alpaca_broker_integration.py::test_connection_with_valid_credentials -v
```

### Skip integration tests (run only unit tests):

```bash
pytest -m "not integration" -v
```

## Test Categories

### Connection Tests

- `test_connection_with_valid_credentials` - Verifies successful connection
- `test_connection_with_invalid_credentials` - Tests error handling
- `test_disconnect_and_reconnect` - Tests connection lifecycle

### Data Retrieval Tests

- `test_get_account_info` - Fetches real account data
- `test_get_historical_data` - Retrieves OHLCV data
- `test_historical_data_pagination` - Tests large dataset handling
- `test_historical_data_different_timeframes` - Tests various timeframes

### Order Management Tests

- `test_get_open_orders` - Lists open orders
- `test_get_open_orders_filtered_by_symbol` - Tests filtering

### Robustness Tests

- `test_rate_limiting_handling` - Verifies rate limit respect
- `test_concurrent_operations` - Tests thread safety
- `test_order_conversion_functions` - Validates data conversion

## CI/CD Considerations

Integration tests can be resource-intensive and may fail due to external factors (API downtime, rate limits, network issues). Consider:

1. **Separate from unit tests**: Run integration tests in a separate CI job
2. **Schedule**: Run on a schedule rather than every commit
3. **Retry logic**: Implement retry for flaky tests
4. **Rate limiting**: Respect API rate limits in CI
5. **Secrets**: Securely store API credentials

Example CI configuration:

```yaml
integration-tests:
  runs-on: ubuntu-latest
  if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'
  steps:
    - uses: actions/checkout@v2
    - name: Run integration tests
      env:
        ALPACA_API_KEY: ${{ secrets.ALPACA_API_KEY }}
        ALPACA_SECRET_KEY: ${{ secrets.ALPACA_SECRET_KEY }}
      run: pytest tests/integration/ -v
```

## Best Practices

1. **Cleanup**: Always clean up test data (cancel orders, etc.)
2. **Idempotency**: Tests should be repeatable without side effects
3. **Isolation**: Tests should not depend on each other
4. **Rate limits**: Add delays between requests to respect API limits
5. **Paper trading only**: Never use live trading credentials

## Troubleshooting

### "Authentication failed"

- Verify API credentials in `.env.test`
- Ensure using paper trading keys
- Check key has not expired

### "Rate limit exceeded"

- Add delays between test runs
- Reduce number of concurrent tests
- Contact Alpaca for increased limits

### "Connection timeout"

- Check internet connection
- Verify Alpaca API status
- Try increasing timeout values

### Tests are flaky

- Add retry logic for transient failures
- Increase delays between operations
- Check for API maintenance windows

## Adding New Integration Tests

When adding new integration tests:

1. Mark with `@pytest.mark.integration`
2. Use `@pytest.mark.asyncio(loop_scope="session")` for async tests
3. Include proper cleanup in fixtures
4. Add delays to respect rate limits
5. Document what the test verifies
6. Update this README

Example:

```python
@pytest.mark.integration
@pytest.mark.asyncio(loop_scope="session")
async def test_new_feature(broker):
    """Test description of what this verifies."""
    # Test implementation
    result = broker.new_feature()
    assert result is not None

    # Cleanup if needed
    await asyncio.sleep(0.5)  # Rate limit respect
```

## Contact

For issues with integration tests, contact the development team or check:

- Alpaca API documentation: https://alpaca.markets/docs/
- Alpaca API status: https://status.alpaca.markets/
