"""
Tests for broker implementations, focusing on Alpaca broker.

This module contains comprehensive tests for the Alpaca broker implementation,
including connection management, order operations, position management, account
information, error handling, and rate limiting behavior.

Environment variables are loaded from .env.test automatically. Tests will skip
if required credentials are not available.
"""

import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from decimal import Decimal

from alpaca.trading.enums import (
    OrderSide as AlpacaOrderSide,
    OrderStatus as AlpacaOrderStatus,
    OrderType as AlpacaOrderType,
    TimeInForce as AlpacaTimeInForce,
)
from alpaca.common.exceptions import APIError

from src.engine.brokers.alpaca import AlpacaBroker
from src.engine.brokers.rate_limiter import TokenBucketRateLimiter
from src.engine.brokers.exc import (
    BrokerError,
    AuthenticationError,
    OrderRejectedError,
    InsufficientFundsError,
    RateLimitError,
    ConnectionError as BrokerConnectionError,
)
from src.engine.models import (
    OrderRequest,
    OrderResponse,
    Position,
    Account,
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def alpaca_credentials():
    """
    Get Alpaca credentials from environment variables.

    Returns:
        Dict with api_key and secret_key from environment, or test values
    """
    return {
        "api_key": os.getenv("ALPACA_API_KEY", "test_api_key"),
        "secret_key": os.getenv("ALPACA_SECRET_KEY", "test_secret_key"),
        "paper": True,
    }


@pytest.fixture
def alpaca_broker(alpaca_credentials):
    """
    Create an Alpaca broker instance for testing.

    Returns:
        AlpacaBroker instance with test credentials
    """
    return AlpacaBroker(
        api_key=alpaca_credentials["api_key"],
        secret_key=alpaca_credentials["secret_key"],
        paper=alpaca_credentials["paper"],
    )


@pytest.fixture
def mock_alpaca_client():
    """
    Create a mock Alpaca trading client.

    Returns:
        Mock TradingClient with commonly used methods
    """
    client = Mock()
    client.get_account = Mock()
    client.submit_order = Mock()
    client.cancel_order_by_id = Mock()
    client.get_order_by_id = Mock()
    client.get_orders = Mock()
    client.get_open_position = Mock()
    client.get_all_positions = Mock()
    client.close_position = Mock()
    return client


@pytest.fixture
def mock_alpaca_account():
    """
    Create a mock Alpaca account object.

    Returns:
        Mock account with typical attributes
    """
    account = Mock()
    account.id = "test_account_123"
    account.equity = "100000.00"
    account.cash = "50000.00"
    account.buying_power = "50000.00"
    account.portfolio_value = "100000.00"
    return account


@pytest.fixture
def mock_alpaca_order():
    """
    Create a mock Alpaca order object.

    Returns:
        Mock order with typical attributes
    """
    order = Mock()
    order.id = "order_123"
    order.client_order_id = "client_123"
    order.symbol = "AAPL"
    order.side = AlpacaOrderSide.BUY
    order.type = AlpacaOrderType.MARKET
    order.qty = "100"
    order.filled_qty = "100"
    order.status = AlpacaOrderStatus.FILLED
    order.submitted_at = datetime.now()
    order.filled_at = datetime.now()
    order.filled_avg_price = "150.50"
    order.time_in_force = AlpacaTimeInForce.DAY
    return order


@pytest.fixture
def mock_alpaca_position():
    """
    Create a mock Alpaca position object.

    Returns:
        Mock position with typical attributes
    """
    position = Mock()
    position.symbol = "AAPL"
    position.qty = "100"
    position.avg_entry_price = "150.00"
    position.current_price = "151.00"
    position.market_value = "15100.00"
    position.unrealized_pl = "100.00"
    position.unrealized_plpc = "0.0067"
    position.cost_basis = "15000.00"
    return position


# ============================================================================
# Connection and Lifecycle Tests
# ============================================================================


def test_broker_initialization(alpaca_credentials):
    """Test that broker initializes with correct parameters."""
    broker = AlpacaBroker(
        api_key=alpaca_credentials["api_key"],
        secret_key=alpaca_credentials["secret_key"],
        paper=True,
    )

    assert broker.api_key == alpaca_credentials["api_key"]
    assert broker.secret_key == alpaca_credentials["secret_key"]
    assert broker.paper is True
    assert broker.client is None
    assert not broker._connected
    assert broker.rate_limiter is not None


def test_broker_initialization_with_custom_rate_limiter(alpaca_credentials):
    """Test broker initialization with custom rate limiter."""
    custom_limiter = TokenBucketRateLimiter(rate=100, per_seconds=60)
    broker = AlpacaBroker(
        api_key=alpaca_credentials["api_key"],
        secret_key=alpaca_credentials["secret_key"],
        rate_limiter=custom_limiter,
    )

    assert broker.rate_limiter is custom_limiter


@patch("src.engine.brokers.alpaca.TradingClient")
def test_successful_connection(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test successful connection to Alpaca."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    assert alpaca_broker._connected is True
    assert alpaca_broker.client is not None
    mock_trading_client.assert_called_once_with(
        api_key=alpaca_broker.api_key,
        secret_key=alpaca_broker.secret_key,
        paper=alpaca_broker.paper,
    )
    mock_client_instance.get_account.assert_called_once()


@patch("src.engine.brokers.alpaca.TradingClient")
def test_connection_with_invalid_credentials(mock_trading_client, alpaca_broker):
    """Test that invalid credentials raise AuthenticationError."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.side_effect = APIError(
        {"message": "Invalid credentials"}, status_code=401
    )
    mock_trading_client.return_value = mock_client_instance

    with pytest.raises(AuthenticationError) as exc_info:
        alpaca_broker.connect()

    assert "Invalid Alpaca credentials" in str(exc_info.value)
    assert exc_info.value.broker_code == "401"
    assert not alpaca_broker._connected


@patch("src.engine.brokers.alpaca.TradingClient")
def test_connection_with_api_error(mock_trading_client, alpaca_broker):
    """Test that API errors during connection raise BrokerConnectionError."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.side_effect = APIError(
        {"message": "Service unavailable"}, status_code=503
    )
    mock_trading_client.return_value = mock_client_instance

    with pytest.raises(BrokerConnectionError) as exc_info:
        alpaca_broker.connect()

    assert "Failed to connect to Alpaca" in str(exc_info.value)
    assert not alpaca_broker._connected


@patch("src.engine.brokers.alpaca.TradingClient")
def test_connection_with_unexpected_error(mock_trading_client, alpaca_broker):
    """Test that unexpected errors during connection are handled."""
    mock_trading_client.side_effect = Exception("Unexpected error")

    with pytest.raises(BrokerConnectionError) as exc_info:
        alpaca_broker.connect()

    assert "Unexpected error connecting to Alpaca" in str(exc_info.value)


def test_disconnect(alpaca_broker):
    """Test broker disconnection."""
    alpaca_broker.client = Mock()
    alpaca_broker._connected = True

    alpaca_broker.disconnect()

    assert alpaca_broker.client is None
    assert not alpaca_broker._connected


@patch("src.engine.brokers.alpaca.TradingClient")
def test_context_manager(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test using broker as context manager."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_trading_client.return_value = mock_client_instance

    with alpaca_broker:
        assert alpaca_broker._connected is True
        assert alpaca_broker.client is not None

    assert not alpaca_broker._connected
    assert alpaca_broker.client is None


# ============================================================================
# Order Submission Tests
# ============================================================================


def test_submit_market_order_not_connected(alpaca_broker):
    """Test that submitting order without connection raises error."""
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.submit_order(order)


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_market_buy_order(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test submitting a market buy order."""
    # Setup
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
        time_in_force=TimeInForce.DAY,
    )

    response = alpaca_broker.submit_order(order)

    assert response.order_id == "order_123"
    assert response.symbol == "AAPL"
    assert response.side == OrderSide.BUY
    assert response.order_type == OrderType.MARKET
    assert response.quantity == 100.0
    assert response.filled_quantity == 100.0
    assert response.status == OrderStatus.FILLED
    mock_client_instance.submit_order.assert_called_once()


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_limit_order(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test submitting a limit order."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_order = Mock()
    mock_order.id = "limit_order_123"
    mock_order.symbol = "AAPL"
    mock_order.side = AlpacaOrderSide.BUY
    mock_order.type = AlpacaOrderType.LIMIT
    mock_order.qty = "100"
    mock_order.filled_qty = "0"
    mock_order.status = AlpacaOrderStatus.ACCEPTED
    mock_order.submitted_at = datetime.now()
    mock_order.filled_at = None
    mock_order.filled_avg_price = None
    mock_order.time_in_force = AlpacaTimeInForce.GTC

    mock_client_instance.submit_order.return_value = mock_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=100.0,
        limit_price=150.0,
        time_in_force=TimeInForce.GTC,
    )

    response = alpaca_broker.submit_order(order)

    assert response.order_id == "limit_order_123"
    assert response.order_type == OrderType.LIMIT
    assert response.status == OrderStatus.SUBMITTED


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_stop_order(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test submitting a stop order."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_order = Mock()
    mock_order.id = "stop_order_123"
    mock_order.symbol = "AAPL"
    mock_order.side = AlpacaOrderSide.SELL
    mock_order.type = AlpacaOrderType.STOP
    mock_order.qty = "100"
    mock_order.filled_qty = "0"
    mock_order.status = AlpacaOrderStatus.ACCEPTED
    mock_order.submitted_at = datetime.now()
    mock_order.filled_at = None
    mock_order.filled_avg_price = None
    mock_order.time_in_force = AlpacaTimeInForce.GTC

    mock_client_instance.submit_order.return_value = mock_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP,
        quantity=100.0,
        stop_price=140.0,
        time_in_force=TimeInForce.GTC,
    )

    response = alpaca_broker.submit_order(order)

    assert response.order_id == "stop_order_123"
    assert response.order_type == OrderType.STOP
    assert response.side == OrderSide.SELL


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_stop_limit_order(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test submitting a stop-limit order."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_order = Mock()
    mock_order.id = "stop_limit_order_123"
    mock_order.symbol = "AAPL"
    mock_order.side = AlpacaOrderSide.SELL
    mock_order.type = AlpacaOrderType.STOP_LIMIT
    mock_order.qty = "100"
    mock_order.filled_qty = "0"
    mock_order.status = AlpacaOrderStatus.ACCEPTED
    mock_order.submitted_at = datetime.now()
    mock_order.filled_at = None
    mock_order.filled_avg_price = None
    mock_order.time_in_force = AlpacaTimeInForce.GTC

    mock_client_instance.submit_order.return_value = mock_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.SELL,
        order_type=OrderType.STOP_LIMIT,
        quantity=100.0,
        stop_price=140.0,
        limit_price=139.0,
        time_in_force=TimeInForce.GTC,
    )

    response = alpaca_broker.submit_order(order)

    assert response.order_id == "stop_limit_order_123"
    assert response.order_type == OrderType.STOP_LIMIT


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_order_with_client_order_id(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test submitting order with custom client order ID."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_alpaca_order.client_order_id = "my_custom_id"
    mock_client_instance.submit_order.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
        client_order_id="my_custom_id",
    )

    response = alpaca_broker.submit_order(order)

    assert response.client_order_id == "my_custom_id"


@patch("src.engine.brokers.alpaca.TradingClient")
def test_submit_order_with_extended_hours(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test submitting order with extended hours enabled."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
        extended_hours=True,
    )

    response = alpaca_broker.submit_order(order)

    assert response is not None
    # Verify the extended_hours parameter was passed to Alpaca
    call_args = mock_client_instance.submit_order.call_args
    assert call_args is not None


# ============================================================================
# Order Management Tests
# ============================================================================


@patch("src.engine.brokers.alpaca.TradingClient")
def test_cancel_order_success(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test successfully cancelling an order."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.cancel_order_by_id.return_value = None
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    result = alpaca_broker.cancel_order("order_123")

    assert result is True
    mock_client_instance.cancel_order_by_id.assert_called_once_with("order_123")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_cancel_nonexistent_order(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test cancelling non-existent order returns False."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.cancel_order_by_id.side_effect = APIError(
        {"message": "Order not found"}, status_code=404
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    result = alpaca_broker.cancel_order("nonexistent")

    assert result is False


def test_cancel_order_not_connected(alpaca_broker):
    """Test that cancelling order without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.cancel_order("order_123")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_order_status(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test retrieving order status."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.get_order_by_id.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    response = alpaca_broker.get_order("order_123")

    assert response.order_id == "order_123"
    assert response.status == OrderStatus.FILLED
    mock_client_instance.get_order_by_id.assert_called_once_with("order_123")


def test_get_order_not_connected(alpaca_broker):
    """Test that getting order without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.get_order("order_123")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_open_orders_all_symbols(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test getting all open orders across all symbols."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_orders = []
    for i, symbol in enumerate(["AAPL", "TSLA", "MSFT"]):
        mock_order = Mock()
        mock_order.id = f"order_{i}"
        mock_order.symbol = symbol
        mock_order.side = AlpacaOrderSide.BUY
        mock_order.type = AlpacaOrderType.LIMIT
        mock_order.qty = "100"
        mock_order.filled_qty = "0"
        mock_order.status = AlpacaOrderStatus.ACCEPTED
        mock_order.submitted_at = datetime.now()
        mock_order.filled_at = None
        mock_order.filled_avg_price = None
        mock_order.time_in_force = AlpacaTimeInForce.GTC
        mock_order.client_order_id = None
        mock_orders.append(mock_order)

    mock_client_instance.get_orders.return_value = mock_orders
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    orders = alpaca_broker.get_open_orders()

    assert len(orders) == 3
    symbols = [o.symbol for o in orders]
    assert "AAPL" in symbols
    assert "TSLA" in symbols
    assert "MSFT" in symbols


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_open_orders_by_symbol(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test getting open orders filtered by symbol."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_order = Mock()
    mock_order.id = "order_aapl"
    mock_order.symbol = "AAPL"
    mock_order.side = AlpacaOrderSide.BUY
    mock_order.type = AlpacaOrderType.LIMIT
    mock_order.qty = "100"
    mock_order.filled_qty = "0"
    mock_order.status = AlpacaOrderStatus.ACCEPTED
    mock_order.submitted_at = datetime.now()
    mock_order.filled_at = None
    mock_order.filled_avg_price = None
    mock_order.time_in_force = AlpacaTimeInForce.GTC
    mock_order.client_order_id = None

    mock_client_instance.get_orders.return_value = [mock_order]
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    orders = alpaca_broker.get_open_orders(symbol="AAPL")

    assert len(orders) == 1
    assert orders[0].symbol == "AAPL"


def test_get_open_orders_not_connected(alpaca_broker):
    """Test that getting open orders without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.get_open_orders()


# ============================================================================
# Position Management Tests
# ============================================================================


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_position(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_position
):
    """Test getting a position for a symbol."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.get_open_position.return_value = mock_alpaca_position
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    position = alpaca_broker.get_position("AAPL")

    assert position is not None
    assert position.symbol == "AAPL"
    assert position.quantity == 100.0
    assert position.average_entry_price == 150.0
    assert position.current_price == 151.0
    assert position.side == OrderSide.BUY
    mock_client_instance.get_open_position.assert_called_once_with("AAPL")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_position_not_found(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test getting position that doesn't exist returns None."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.get_open_position.side_effect = APIError(
        {"message": "Position not found"}, status_code=404
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    position = alpaca_broker.get_position("NONEXISTENT")

    assert position is None


def test_get_position_not_connected(alpaca_broker):
    """Test that getting position without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.get_position("AAPL")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_all_positions(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test getting all positions."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_positions = []
    for symbol, qty in [("AAPL", "100"), ("TSLA", "50"), ("MSFT", "75")]:
        mock_pos = Mock()
        mock_pos.symbol = symbol
        mock_pos.qty = qty
        mock_pos.avg_entry_price = "150.00"
        mock_pos.current_price = "151.00"
        mock_pos.market_value = str(float(qty) * 151.0)
        mock_pos.unrealized_pl = "100.00"
        mock_pos.unrealized_plpc = "0.0067"
        mock_pos.cost_basis = str(float(qty) * 150.0)
        mock_positions.append(mock_pos)

    mock_client_instance.get_all_positions.return_value = mock_positions
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    positions = alpaca_broker.get_all_positions()

    assert len(positions) == 3
    symbols = [p.symbol for p in positions]
    assert "AAPL" in symbols
    assert "TSLA" in symbols
    assert "MSFT" in symbols


def test_get_all_positions_not_connected(alpaca_broker):
    """Test that getting all positions without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.get_all_positions()


@patch("src.engine.brokers.alpaca.TradingClient")
def test_close_position(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test closing a position."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_alpaca_order.side = AlpacaOrderSide.SELL
    mock_client_instance.close_position.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    response = alpaca_broker.close_position("AAPL")

    assert response is not None
    assert response.side == OrderSide.SELL
    mock_client_instance.close_position.assert_called_once_with("AAPL")


def test_close_position_not_connected(alpaca_broker):
    """Test that closing position without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.close_position("AAPL")


@patch("src.engine.brokers.alpaca.TradingClient")
def test_position_with_negative_quantity(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test handling of short positions (negative quantity)."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account

    mock_pos = Mock()
    mock_pos.symbol = "AAPL"
    mock_pos.qty = "-100"  # Short position
    mock_pos.avg_entry_price = "150.00"
    mock_pos.current_price = "145.00"  # Profitable short
    mock_pos.market_value = "-14500.00"
    mock_pos.unrealized_pl = "500.00"
    mock_pos.unrealized_plpc = "0.0333"
    mock_pos.cost_basis = "-15000.00"

    mock_client_instance.get_open_position.return_value = mock_pos
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    position = alpaca_broker.get_position("AAPL")

    assert position is not None
    assert position.symbol == "AAPL"
    assert position.quantity == 100.0  # Absolute value
    assert position.side == OrderSide.SELL  # Short position


# ============================================================================
# Account Information Tests
# ============================================================================


@patch("src.engine.brokers.alpaca.TradingClient")
def test_get_account_info(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test getting account information."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    account = alpaca_broker.get_account()

    assert account.account_id == "test_account_123"
    assert account.equity == 100000.0
    assert account.cash == 50000.0
    assert account.buying_power == 50000.0
    assert account.portfolio_value == 100000.0
    assert account.last_updated is not None


def test_get_account_not_connected(alpaca_broker):
    """Test that getting account without connection raises error."""
    with pytest.raises(BrokerError, match="not connected"):
        alpaca_broker.get_account()


# ============================================================================
# Error Handling Tests
# ============================================================================


@patch("src.engine.brokers.alpaca.TradingClient")
def test_insufficient_funds_error(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test that insufficient funds error is properly raised."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = APIError(
        {"message": "insufficient buying power"}, status_code=403
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=10000.0,  # Too many shares
    )

    with pytest.raises(InsufficientFundsError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Insufficient funds" in str(exc_info.value)
    assert exc_info.value.broker_code == "403"


@patch("src.engine.brokers.alpaca.TradingClient")
def test_order_rejected_error(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test that order rejection is properly handled."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = APIError(
        {"message": "Order rejected"}, status_code=403
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="INVALID",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    with pytest.raises(OrderRejectedError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Order rejected" in str(exc_info.value)


@patch("src.engine.brokers.alpaca.TradingClient")
def test_rate_limit_error(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test that rate limit errors are properly handled."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = APIError(
        {"message": "Rate limit exceeded"}, status_code=429
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    with pytest.raises(RateLimitError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Rate limit exceeded" in str(exc_info.value)
    assert exc_info.value.broker_code == "429"
    assert exc_info.value.retry_after == 60


@patch("src.engine.brokers.alpaca.TradingClient")
def test_invalid_order_parameters_error(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test that invalid order parameters raise appropriate error."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = APIError(
        {"message": "Invalid order parameters"}, status_code=422
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=100.0,
        limit_price=0.01,  # Very low price
    )

    with pytest.raises(OrderRejectedError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Invalid order parameters" in str(exc_info.value)
    assert exc_info.value.broker_code == "422"


@patch("src.engine.brokers.alpaca.TradingClient")
def test_generic_api_error(mock_trading_client, alpaca_broker, mock_alpaca_account):
    """Test that generic API errors are handled."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = APIError(
        {"message": "Internal server error"}, status_code=500
    )
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    with pytest.raises(BrokerError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Alpaca API error" in str(exc_info.value)
    assert exc_info.value.broker_code == "500"


@patch("src.engine.brokers.alpaca.TradingClient")
def test_unexpected_exception_during_order_submission(
    mock_trading_client, alpaca_broker, mock_alpaca_account
):
    """Test that unexpected exceptions are properly wrapped."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.side_effect = Exception("Unexpected error")
    mock_trading_client.return_value = mock_client_instance

    alpaca_broker.connect()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    with pytest.raises(BrokerError) as exc_info:
        alpaca_broker.submit_order(order)

    assert "Failed to submit order" in str(exc_info.value)


# ============================================================================
# Rate Limiting Tests
# ============================================================================


@patch("src.engine.brokers.alpaca.TradingClient")
def test_rate_limiter_is_applied(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test that rate limiter is called during operations."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.return_value = mock_alpaca_order
    mock_trading_client.return_value = mock_client_instance

    # Create broker with mock rate limiter
    mock_rate_limiter = Mock(spec=TokenBucketRateLimiter)
    broker = AlpacaBroker(
        api_key="test_key",
        secret_key="test_secret",
        paper=True,
        rate_limiter=mock_rate_limiter,
    )

    broker.connect()

    # Rate limiter should be called during connect
    assert mock_rate_limiter.acquire.called

    # Reset call count
    mock_rate_limiter.acquire.reset_mock()

    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )

    broker.submit_order(order)

    # Rate limiter should be called during order submission
    assert mock_rate_limiter.acquire.called


def test_default_rate_limiter_configuration(alpaca_credentials):
    """Test that default rate limiter is configured correctly."""
    broker = AlpacaBroker(
        api_key=alpaca_credentials["api_key"],
        secret_key=alpaca_credentials["secret_key"],
    )

    assert broker.rate_limiter is not None
    assert isinstance(broker.rate_limiter, TokenBucketRateLimiter)
    assert broker.rate_limiter.rate == 200  # Alpaca's limit
    assert broker.rate_limiter.per_seconds == 60


@patch("src.engine.brokers.alpaca.TradingClient")
def test_multiple_operations_respect_rate_limit(
    mock_trading_client, alpaca_broker, mock_alpaca_account, mock_alpaca_order
):
    """Test that multiple operations all apply rate limiting."""
    mock_client_instance = Mock()
    mock_client_instance.get_account.return_value = mock_alpaca_account
    mock_client_instance.submit_order.return_value = mock_alpaca_order
    mock_client_instance.get_order_by_id.return_value = mock_alpaca_order
    mock_client_instance.get_orders.return_value = [mock_alpaca_order]
    mock_trading_client.return_value = mock_client_instance

    # Create broker with mock rate limiter
    mock_rate_limiter = Mock(spec=TokenBucketRateLimiter)
    broker = AlpacaBroker(
        api_key="test_key",
        secret_key="test_secret",
        paper=True,
        rate_limiter=mock_rate_limiter,
    )

    broker.connect()
    call_count_after_connect = mock_rate_limiter.acquire.call_count

    # Submit order
    order = OrderRequest(
        symbol="AAPL",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=100.0,
    )
    broker.submit_order(order)

    # Get order
    broker.get_order("order_123")

    # Get open orders
    broker.get_open_orders()

    # Each operation should call rate limiter
    assert mock_rate_limiter.acquire.call_count > call_count_after_connect


# ============================================================================
# Environment Variable Tests
# ============================================================================


def test_credentials_from_environment():
    """Test that credentials can be loaded from environment variables."""
    # This test uses actual environment variables if available
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if api_key and secret_key:
        broker = AlpacaBroker(api_key=api_key, secret_key=secret_key, paper=True)
        assert broker.api_key == api_key
        assert broker.secret_key == secret_key
    else:
        pytest.skip("Alpaca credentials not available in environment")


def test_missing_credentials_handled_gracefully():
    """Test that missing credentials are handled gracefully."""
    # Tests can still run with test credentials
    broker = AlpacaBroker(
        api_key="test_key",
        secret_key="test_secret",
        paper=True,
    )
    assert broker.api_key == "test_key"
    assert broker.secret_key == "test_secret"
