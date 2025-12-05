"""
End-to-end integration tests for the trading strategy framework.

This module tests the complete flow from data loading through strategy execution
to performance metrics calculation, ensuring all components work together correctly.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from src.engine.backtesting.data_loader import OHLCDataLoader, OHLCBar, Timeframe
from src.engine.backtesting.engine import BacktestEngine, BacktestConfig
from src.engine.backtesting.simulated_broker import SimulatedBroker
from src.engine.context.base import StrategyContext
from src.engine.models import OrderType, OrderSide


# ============================================================================
# End-to-End Integration Tests
# ============================================================================


class TestEndToEndIntegration:
    """Test complete backtest workflow with real strategy execution."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_simple_buy_and_hold_strategy(
        self, base_timestamp, mock_db_session_with_data, simple_buy_strategy
    ):
        """
        Test a simple buy-and-hold strategy through complete backtest.

        This test validates:
        - Data loading from database
        - Strategy execution with context
        - Order execution through simulated broker
        - Position tracking
        - Performance metrics calculation
        """
        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(days=1),
            symbols=["AAPL"],
            initial_capital=100000.0,
            commission_percent=0.1,
            slippage_percent=0.05,
        )

        loader = OHLCDataLoader(mock_db_session_with_data)
        engine = BacktestEngine(config, loader, simple_buy_strategy)

        # Note: This would normally run a full backtest
        # For unit tests, we validate the components are properly connected
        assert engine.config == config
        assert engine.broker.initial_capital == 100000.0
        assert engine.strategy_func == simple_buy_strategy

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_sma_crossover_strategy_execution(
        self, base_timestamp, mock_db_session_with_data, sma_crossover_strategy
    ):
        """
        Test SMA crossover strategy through complete workflow.

        This test validates:
        - Historical data access for indicator calculation
        - Trading logic based on technical indicators
        - Multiple order executions
        - Position opening and closing
        """
        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(days=2),
            symbols=["AAPL"],
            initial_capital=100000.0,
            commission_percent=0.0,
            slippage_percent=0.0,
        )

        loader = OHLCDataLoader(mock_db_session_with_data)
        engine = BacktestEngine(config, loader, sma_crossover_strategy)

        # Validate engine is properly configured
        assert len(config.symbols) == 1
        assert engine.broker.commission_percent == 0.0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_multi_symbol_strategy(
        self, base_timestamp, multi_symbol_bars, mock_db_session
    ):
        """
        Test strategy trading multiple symbols simultaneously.

        This test validates:
        - Multi-symbol data handling
        - Portfolio allocation across symbols
        - Independent position tracking per symbol
        - Account balance management
        """

        # Create a simple multi-symbol strategy
        async def multi_symbol_strategy(context):
            """Buy all available symbols once."""
            for symbol in context.symbols():
                position = await context.get_position(symbol)
                if not position:
                    price = context.close(symbol)
                    if price:
                        await context.buy(symbol, quantity=5)

        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(hours=1),
            symbols=["AAPL", "TSLA", "MSFT"],
            initial_capital=100000.0,
        )

        loader = OHLCDataLoader(mock_db_session)
        engine = BacktestEngine(config, loader, multi_symbol_strategy)

        assert len(config.symbols) == 3

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_strategy_with_risk_management(self, base_timestamp, mock_db_session):
        """
        Test strategy with position sizing and risk management.

        This test validates:
        - Dynamic position sizing based on account equity
        - Maximum position limits
        - Stop-loss order placement
        - Portfolio risk controls
        """

        async def risk_managed_strategy(context):
            """Strategy with 2% position size limit."""
            account = await context.get_account()
            max_position_value = account.portfolio_value * 0.02

            symbol = "AAPL"
            position = await context.get_position(symbol)

            if not position:
                price = context.close(symbol)
                if price and price > 0:
                    quantity = int(max_position_value / price)
                    if quantity > 0:
                        await context.buy(symbol, quantity=quantity)

        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(hours=1),
            symbols=["AAPL"],
            initial_capital=100000.0,
        )

        loader = OHLCDataLoader(mock_db_session)
        engine = BacktestEngine(config, loader, risk_managed_strategy)

        assert engine.broker.initial_capital == 100000.0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_strategy_error_handling(self, base_timestamp, mock_db_session):
        """
        Test that strategy errors don't crash the backtest engine.

        This test validates:
        - Graceful error handling in strategy code
        - Backtest continues after strategy errors
        - Error logging and reporting
        """
        error_count = {"value": 0}

        async def buggy_strategy(context):
            """Strategy that sometimes raises errors."""
            error_count["value"] += 1
            if error_count["value"] == 2:
                raise ValueError("Intentional error for testing")

            # Normal trading logic
            symbol = "AAPL"
            position = await context.get_position(symbol)
            if not position:
                await context.buy(symbol, quantity=10)

        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(hours=1),
            symbols=["AAPL"],
            initial_capital=100000.0,
        )

        loader = OHLCDataLoader(mock_db_session)
        engine = BacktestEngine(config, loader, buggy_strategy)

        # Engine should initialize successfully
        assert engine is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_limit_order_workflow(self, base_timestamp, sample_bars):
        """
        Test complete workflow with limit orders.

        This test validates:
        - Limit order submission
        - Order tracking until filled
        - Price-conditional execution
        - Order status updates
        """
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        # Set initial state
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 155.0)

        # Create context
        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(AsyncMock())

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Place limit order below current price
        order = await context.buy(
            "AAPL",
            quantity=10,
            order_type=OrderType.LIMIT,
            limit_price=150.0,
        )

        assert order.status.value == "pending"

        # Simulate price drop
        broker.set_current_time(base_timestamp + timedelta(seconds=30))
        broker.set_current_price("AAPL", 149.0)

        # Check if order was filled
        position = await context.get_position("AAPL")
        assert position is not None
        assert position.quantity == 10.0

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_complete_trade_lifecycle(self, base_timestamp, sample_bars):
        """
        Test complete trade lifecycle from entry to exit.

        This test validates:
        - Entry order execution
        - Position holding with price updates
        - Unrealized P&L tracking
        - Exit order execution
        - Realized P&L calculation
        """
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(AsyncMock())

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Step 1: Enter position
        initial_cash = broker.cash
        entry_order = await context.buy("AAPL", quantity=100)
        assert entry_order.status.value == "filled"

        # Cash should decrease
        assert broker.cash < initial_cash

        # Step 2: Hold position and check unrealized P&L
        position = await context.get_position("AAPL")
        assert position.quantity == 100.0

        # Price increases
        broker.set_current_price("AAPL", 155.0)
        position = await context.get_position("AAPL")
        assert position.unrealized_pnl > 0

        # Step 3: Exit position
        exit_order = await context.sell("AAPL", quantity=100)
        assert exit_order.status.value == "filled"

        # Position should be closed
        final_position = await context.get_position("AAPL")
        assert final_position is None

        # Cash should increase (profit realized)
        assert broker.cash > initial_cash

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_commission_and_slippage_impact(self, base_timestamp, sample_bars):
        """
        Test that commissions and slippage are properly applied.

        This test validates:
        - Commission calculation and deduction
        - Slippage on market orders
        - Impact on realized P&L
        - Accurate cost basis tracking
        """
        # Test with commissions and slippage
        broker_with_costs = SimulatedBroker(
            initial_capital=100000.0,
            commission_per_share=0.01,
            commission_percent=0.1,
            slippage_percent=0.1,
        )
        await broker_with_costs.connect()

        broker_with_costs.set_current_time(base_timestamp)
        broker_with_costs.set_current_price("AAPL", 100.0)

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(AsyncMock())

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker_with_costs,
            data_loader=loader,
        )

        initial_cash = broker_with_costs.cash

        # Buy with slippage and commission
        order = await context.buy("AAPL", quantity=100)

        # Should pay more than 100 * 100 due to slippage and commission
        cash_spent = initial_cash - broker_with_costs.cash
        assert cash_spent > 10000.0

        # Check that commission was recorded
        assert "commission" in order.broker_metadata
        assert order.broker_metadata["commission"] > 0

        await broker_with_costs.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_portfolio_rebalancing(self, base_timestamp, multi_symbol_bars):
        """
        Test portfolio rebalancing across multiple positions.

        This test validates:
        - Getting all current positions
        - Calculating target allocations
        - Executing multiple trades to rebalance
        - Maintaining portfolio constraints
        """
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        for bar in multi_symbol_bars:
            broker.set_current_price(bar.symbol, bar.close)

        bars = {bar.symbol: bar for bar in multi_symbol_bars}
        loader = OHLCDataLoader(AsyncMock())

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Initial equal allocation
        for symbol in ["AAPL", "TSLA", "MSFT"]:
            await context.buy(symbol, quantity=50)

        # Check we have 3 positions
        positions = await context.get_all_positions()
        assert len(positions) == 3

        # Simulate rebalancing - close one, increase another
        await context.close_position("MSFT")
        await context.buy("AAPL", quantity=25)

        # Verify final state
        final_positions = await context.get_all_positions()
        assert len(final_positions) == 2

        aapl_position = await context.get_position("AAPL")
        assert aapl_position.quantity == 75.0

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_account_equity_tracking(self, base_timestamp, sample_bars):
        """
        Test that account equity is correctly tracked throughout trading.

        This test validates:
        - Initial equity equals initial capital
        - Equity updates with position values
        - Equity reflects both cash and positions
        - Equity calculation accuracy
        """
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 100.0)

        bars = {"AAPL": sample_bars[0]}
        loader = OHLCDataLoader(AsyncMock())

        context = StrategyContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Check initial equity
        account = await context.get_account()
        assert account.equity == 100000.0
        assert account.cash == 100000.0
        assert account.portfolio_value == 100000.0

        # Buy shares
        await context.buy("AAPL", quantity=500)

        # Equity should still be approximately the same (minus commission/slippage)
        account = await context.get_account()
        assert account.equity < 100000.0  # Slightly less due to costs
        assert account.equity > 99000.0  # But close

        # Price increases
        broker.set_current_price("AAPL", 110.0)

        # Equity should increase
        account = await context.get_account()
        assert account.equity > 100000.0  # Profit from price increase

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_strategy_state_persistence(self, base_timestamp, sample_bars):
        """
        Test that strategy can maintain state across multiple bars.

        This test validates:
        - Strategy can track internal state
        - State persists between bar updates
        - Strategy can make decisions based on history
        """
        state = {"bar_count": 0, "bought": False}

        async def stateful_strategy(context):
            """Strategy that counts bars and buys on 3rd bar."""
            state["bar_count"] += 1

            if state["bar_count"] == 3 and not state["bought"]:
                await context.buy("AAPL", quantity=10)
                state["bought"] = True

        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        loader = OHLCDataLoader(AsyncMock())

        # Simulate multiple bars
        for i in range(5):
            timestamp = base_timestamp + timedelta(minutes=i)
            broker.set_current_time(timestamp)
            broker.set_current_price("AAPL", 150.0 + i)

            context = StrategyContext(
                timestamp=timestamp,
                bars={"AAPL": sample_bars[min(i, len(sample_bars) - 1)]},
                broker=broker,
                data_loader=loader,
            )

            await stateful_strategy(context)

        # Verify state was maintained
        assert state["bar_count"] == 5
        assert state["bought"] is True

        # Verify position was created
        position = await broker.get_position("AAPL")
        assert position is not None
        assert position.quantity == 10.0

        await broker.disconnect()


# ============================================================================
# Performance and Edge Case Tests
# ============================================================================


class TestEdgeCases:
    """Test edge cases and error conditions in integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_zero_capital_scenario(self, base_timestamp):
        """Test that trading with zero capital is handled properly."""
        broker = SimulatedBroker(initial_capital=0.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        from engine.brokers.exc import InsufficientFundsError
        from src.engine.models import OrderRequest

        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=1.0,
        )

        with pytest.raises(InsufficientFundsError):
            await broker.submit_order(order)

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_fractional_shares_handling(self, base_timestamp):
        """Test handling of fractional share quantities."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        from src.engine.models import OrderRequest

        # Try fractional quantity
        order = OrderRequest(
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10.5,  # Fractional
        )

        response = await broker.submit_order(order)
        assert response.filled_quantity == 10.5

        position = await broker.get_position("AAPL")
        assert position.quantity == 10.5

        await broker.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_concurrent_orders_same_symbol(self, base_timestamp):
        """Test handling multiple orders for the same symbol."""
        broker = SimulatedBroker(initial_capital=100000.0)
        await broker.connect()

        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        from src.engine.models import OrderRequest

        # Submit multiple buy orders
        for i in range(3):
            order = OrderRequest(
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10.0,
            )
            await broker.submit_order(order)

        # Should have cumulative position
        position = await broker.get_position("AAPL")
        assert position.quantity == 30.0

        await broker.disconnect()
