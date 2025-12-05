"""
Tests for backtesting system: data loader, engine, and metrics.

This module tests the OHLC data loader, backtest engine orchestration,
and performance metrics calculations.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import numpy as np

from src.engine.backtesting.data_loader import (
    OHLCDataLoader,
    OHLCBar,
    Timeframe,
    TradeRecord,
)
from src.engine.backtesting.engine import (
    BacktestEngine,
    BacktestConfig,
    BacktestContext,
)
from src.engine.backtesting.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_win_rate,
    calculate_total_return,
    calculate_average_trade,
    calculate_profit_factor,
    calculate_sortino_ratio,
    calculate_calmar_ratio,
    calculate_recovery_factor,
    calculate_equity_curve_stats,
    calculate_trade_stats,
)
from engine.brokers.simulated_broker import BacktestBroker
from src.engine.models import OrderSide


# ============================================================================
# Data Loader Tests
# ============================================================================


class TestOHLCDataLoader:
    """Test the OHLC data loader for fetching historical data."""

    @pytest.mark.asyncio
    async def test_data_loader_initialization(self, mock_db_session):
        """Test that data loader initializes with database session."""
        loader = OHLCDataLoader(mock_db_session)

        assert loader.db_session == mock_db_session

    @pytest.mark.asyncio
    async def test_load_data_with_valid_parameters(
        self, mock_db_session_with_data, base_timestamp, sample_bars
    ):
        """Test loading data with valid parameters."""
        loader = OHLCDataLoader(mock_db_session_with_data)

        end_date = base_timestamp + timedelta(days=1)

        batches = []
        async for batch in loader.load_data(
            symbols=["AAPL"],
            start_date=base_timestamp,
            end_date=end_date,
            timeframe=Timeframe.M1,
        ):
            batches.append(batch)

        assert len(batches) > 0
        assert all(isinstance(bar, OHLCBar) for batch in batches for bar in batch)

    @pytest.mark.asyncio
    async def test_load_data_invalid_symbols_raises_error(
        self, mock_db_session, base_timestamp
    ):
        """Test that empty symbols list raises ValueError."""
        loader = OHLCDataLoader(mock_db_session)

        with pytest.raises(ValueError, match="At least one symbol must be provided"):
            async for _ in loader.load_data(
                symbols=[],
                start_date=base_timestamp,
                end_date=base_timestamp + timedelta(days=1),
                timeframe=Timeframe.M1,
            ):
                pass

    @pytest.mark.asyncio
    async def test_load_data_invalid_date_range_raises_error(
        self, mock_db_session, base_timestamp
    ):
        """Test that invalid date range raises ValueError."""
        loader = OHLCDataLoader(mock_db_session)

        with pytest.raises(ValueError, match="start_date must be before end_date"):
            async for _ in loader.load_data(
                symbols=["AAPL"],
                start_date=base_timestamp + timedelta(days=1),
                end_date=base_timestamp,
                timeframe=Timeframe.M1,
            ):
                pass

    @pytest.mark.asyncio
    async def test_get_bar_count(self, mock_db_session, base_timestamp):
        """Test getting total bar count."""
        loader = OHLCDataLoader(mock_db_session)

        count = await loader.get_bar_count(
            symbols=["AAPL"],
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(days=1),
            timeframe=Timeframe.M1,
        )

        assert isinstance(count, int)
        assert count >= 0

    def test_ohlc_bar_creation(self, base_timestamp):
        """Test creating an OHLC bar."""
        bar = OHLCBar(
            symbol="AAPL",
            timestamp=base_timestamp,
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000,
            timeframe=Timeframe.M1,
        )

        assert bar.symbol == "AAPL"
        assert bar.timestamp == base_timestamp
        assert bar.open == 100.0
        assert bar.high == 105.0
        assert bar.low == 99.0
        assert bar.close == 103.0
        assert bar.volume == 1000000

    def test_trade_record_creation(self, base_timestamp):
        """Test creating a trade record."""
        trade = TradeRecord(
            trade_id="TRADE123",
            symbol="AAPL",
            side=OrderSide.BUY,
            entry_time=base_timestamp,
            entry_price=100.0,
            quantity=10.0,
            exit_time=base_timestamp + timedelta(hours=1),
            exit_price=105.0,
            pnl=50.0,
            commission=1.0,
            slippage=0.5,
        )

        assert trade.trade_id == "TRADE123"
        assert trade.symbol == "AAPL"
        assert trade.pnl == 50.0


# ============================================================================
# Backtest Engine Tests
# ============================================================================


class TestBacktestEngine:
    """Test the backtest engine orchestration."""

    def test_backtest_config_creation(self, base_timestamp):
        """Test creating backtest configuration."""
        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(days=30),
            symbol=["AAPL", "TSLA"],
            starting_balance=100000.0,
            commission_percent=0.1,
            slippage_percent=0.05,
        )

        assert config.starting_balance == 100000.0
        assert len(config.symbol) == 2
        assert config.commission_percent == 0.1
        assert config.slippage_percent == 0.05

    def test_backtest_engine_initialization(
        self, base_timestamp, mock_db_session, simple_buy_strategy
    ):
        """Test initializing backtest engine."""
        config = BacktestConfig(
            start_date=base_timestamp,
            end_date=base_timestamp + timedelta(days=1),
            symbol=["AAPL"],
            starting_balance=100000.0,
        )

        loader = OHLCDataLoader(mock_db_session)
        engine = BacktestEngine(config, loader, simple_buy_strategy)

        assert engine._config == config
        assert engine._data_loader == loader
        assert engine._strategy_func == simple_buy_strategy
        assert isinstance(engine._broker, BacktestBroker)

    @pytest.mark.asyncio
    async def test_backtest_context_provides_bar_access(
        self, base_timestamp, sample_bar, mock_broker, mock_db_session
    ):
        """Test that backtest context provides access to current bar data."""
        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = BacktestContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=mock_broker,
            data_loader=loader,
        )

        assert context.close("AAPL") == 151.0
        assert context.open("AAPL") == 150.0
        assert context.high("AAPL") == 152.0
        assert context.low("AAPL") == 149.0
        assert context.volume("AAPL") == 1000000

    @pytest.mark.asyncio
    async def test_backtest_context_missing_symbol_returns_none(
        self, base_timestamp, sample_bar, mock_broker, mock_db_session
    ):
        """Test that accessing missing symbol returns None."""
        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = BacktestContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=mock_broker,
            data_loader=loader,
        )

        assert context.close("TSLA") is None
        assert context.open("TSLA") is None

    @pytest.mark.asyncio
    async def test_backtest_context_trading_operations(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test that context provides trading operations."""
        broker = BacktestBroker(starting_balance=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)
        broker.set_current_price("AAPL", 150.0)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = BacktestContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        # Test buy
        order = await context.buy("AAPL", quantity=10)
        assert order is not None

        # Test position
        position = await context.position("AAPL")
        assert position is not None
        assert position.quantity == 10.0

        # Test sell
        order = await context.sell("AAPL", quantity=10)
        assert order is not None

        await broker.disconnect()

    @pytest.mark.asyncio
    async def test_backtest_context_account_access(
        self, base_timestamp, sample_bar, mock_db_session
    ):
        """Test that context provides account access."""
        broker = BacktestBroker(starting_balance=100000.0)
        await broker.connect()
        broker.set_current_time(base_timestamp)

        bars = {"AAPL": sample_bar}
        loader = OHLCDataLoader(mock_db_session)

        context = BacktestContext(
            timestamp=base_timestamp,
            bars=bars,
            broker=broker,
            data_loader=loader,
        )

        account = await context.account()
        assert account is not None
        assert account.cash == 100000.0

        await broker.disconnect()


# ============================================================================
# Performance Metrics Tests
# ============================================================================


class TestPerformanceMetrics:
    """Test performance metrics calculations."""

    def test_calculate_sharpe_ratio(self, sample_equity_curve):
        """Test Sharpe ratio calculation."""
        sharpe = calculate_sharpe_ratio(sample_equity_curve)

        assert isinstance(sharpe, float)
        assert not np.isnan(sharpe)
        assert not np.isinf(sharpe)

    def test_calculate_sharpe_ratio_insufficient_data(self):
        """Test Sharpe ratio with insufficient data."""
        equity_curve = [(datetime.now(), 100000.0)]

        sharpe = calculate_sharpe_ratio(equity_curve)
        assert sharpe == 0.0

    def test_calculate_sharpe_ratio_zero_volatility(self, base_timestamp):
        """Test Sharpe ratio with zero volatility."""
        # Flat equity curve
        equity_curve = [
            (base_timestamp + timedelta(days=i), 100000.0) for i in range(10)
        ]

        sharpe = calculate_sharpe_ratio(equity_curve)
        assert sharpe == 0.0

    def test_calculate_max_drawdown(self, volatile_equity_curve):
        """Test maximum drawdown calculation."""
        max_dd, max_dd_pct = calculate_max_drawdown(volatile_equity_curve)

        assert isinstance(max_dd, float)
        assert isinstance(max_dd_pct, float)
        assert max_dd >= 0.0
        assert max_dd_pct >= 0.0

    def test_calculate_max_drawdown_no_drawdown(self, base_timestamp):
        """Test max drawdown with only increasing equity."""
        equity_curve = [
            (base_timestamp + timedelta(days=i), 100000.0 + i * 1000) for i in range(10)
        ]

        max_dd, max_dd_pct = calculate_max_drawdown(equity_curve)
        assert max_dd == 0.0
        assert max_dd_pct == 0.0

    def test_calculate_max_drawdown_empty_curve(self):
        """Test max drawdown with empty equity curve."""
        max_dd, max_dd_pct = calculate_max_drawdown([])

        assert max_dd == 0.0
        assert max_dd_pct == 0.0

    def test_calculate_win_rate(self, base_timestamp):
        """Test win rate calculation."""
        trades = [
            TradeRecord(
                trade_id=f"T{i}",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=50.0 if i % 2 == 0 else -30.0,
            )
            for i in range(10)
        ]

        win_rate = calculate_win_rate(trades)

        assert isinstance(win_rate, float)
        assert 0.0 <= win_rate <= 100.0
        assert win_rate == 50.0  # 5 winners out of 10

    def test_calculate_win_rate_no_trades(self):
        """Test win rate with no trades."""
        win_rate = calculate_win_rate([])
        assert win_rate == 0.0

    def test_calculate_win_rate_all_winners(self, base_timestamp):
        """Test win rate with all winning trades."""
        trades = [
            TradeRecord(
                trade_id=f"T{i}",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=50.0,
            )
            for i in range(5)
        ]

        win_rate = calculate_win_rate(trades)
        assert win_rate == 100.0

    def test_calculate_total_return(self):
        """Test total return calculation."""
        ret_dollars, ret_percent = calculate_total_return(100000.0, 110000.0)

        assert ret_dollars == 10000.0
        assert ret_percent == 10.0

    def test_calculate_total_return_loss(self):
        """Test total return with loss."""
        ret_dollars, ret_percent = calculate_total_return(100000.0, 90000.0)

        assert ret_dollars == -10000.0
        assert ret_percent == -10.0

    def test_calculate_average_trade(self, base_timestamp):
        """Test average trade statistics."""
        trades = [
            TradeRecord(
                trade_id="T1",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=100.0,
            ),
            TradeRecord(
                trade_id="T2",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=-50.0,
            ),
            TradeRecord(
                trade_id="T3",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=75.0,
            ),
        ]

        avg_pnl, avg_win, avg_loss = calculate_average_trade(trades)

        assert isinstance(avg_pnl, float)
        assert isinstance(avg_win, float)
        assert isinstance(avg_loss, float)
        assert avg_pnl == pytest.approx(41.67, rel=0.01)
        assert avg_win == pytest.approx(87.5, rel=0.01)
        assert avg_loss == -50.0

    def test_calculate_profit_factor(self, base_timestamp):
        """Test profit factor calculation."""
        trades = [
            TradeRecord(
                trade_id="T1",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=200.0,
            ),
            TradeRecord(
                trade_id="T2",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=-100.0,
            ),
        ]

        pf = calculate_profit_factor(trades)

        assert isinstance(pf, float)
        assert pf == 2.0  # 200 / 100

    def test_calculate_profit_factor_no_losses(self, base_timestamp):
        """Test profit factor with no losing trades."""
        trades = [
            TradeRecord(
                trade_id="T1",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=100.0,
            ),
        ]

        pf = calculate_profit_factor(trades)
        assert np.isinf(pf)

    def test_calculate_sortino_ratio(self, sample_equity_curve):
        """Test Sortino ratio calculation."""
        sortino = calculate_sortino_ratio(sample_equity_curve)

        assert isinstance(sortino, float)
        # Sortino can be very large or infinite with no downside
        assert not np.isnan(sortino)

    def test_calculate_calmar_ratio(self, sample_equity_curve):
        """Test Calmar ratio calculation."""
        calmar = calculate_calmar_ratio(sample_equity_curve)

        assert isinstance(calmar, float)
        assert not np.isnan(calmar)

    def test_calculate_recovery_factor(self, volatile_equity_curve):
        """Test recovery factor calculation."""
        recovery = calculate_recovery_factor(volatile_equity_curve)

        assert isinstance(recovery, float)
        assert not np.isnan(recovery)

    def test_calculate_equity_curve_stats(self, sample_equity_curve):
        """Test comprehensive equity curve statistics."""
        stats = calculate_equity_curve_stats(sample_equity_curve)

        assert isinstance(stats, dict)
        assert "min_equity" in stats
        assert "max_equity" in stats
        assert "mean_equity" in stats
        assert "std_equity" in stats
        assert "sharpe_ratio" in stats
        assert "sortino_ratio" in stats
        assert "calmar_ratio" in stats
        assert "recovery_factor" in stats

    def test_calculate_equity_curve_stats_empty(self):
        """Test equity curve stats with empty data."""
        stats = calculate_equity_curve_stats([])
        assert stats == {}

    def test_calculate_trade_stats(self, base_timestamp):
        """Test comprehensive trade statistics."""
        trades = [
            TradeRecord(
                trade_id=f"T{i}",
                symbol="AAPL",
                side=OrderSide.BUY,
                entry_time=base_timestamp,
                entry_price=100.0,
                quantity=10.0,
                pnl=100.0 if i % 2 == 0 else -50.0,
                commission=1.0,
                slippage=0.5,
            )
            for i in range(10)
        ]

        stats = calculate_trade_stats(trades)

        assert isinstance(stats, dict)
        assert stats["total_trades"] == 10
        assert stats["winning_trades"] == 5
        assert stats["losing_trades"] == 5
        assert stats["win_rate"] == 50.0
        assert "avg_pnl" in stats
        assert "avg_win" in stats
        assert "avg_loss" in stats
        assert "profit_factor" in stats
        assert "total_pnl" in stats
        assert "total_commission" in stats
        assert "total_slippage" in stats

    def test_calculate_trade_stats_no_trades(self):
        """Test trade stats with no trades."""
        stats = calculate_trade_stats([])

        assert stats["total_trades"] == 0
        assert stats["winning_trades"] == 0
        assert stats["losing_trades"] == 0
        assert stats["win_rate"] == 0.0

    def test_sharpe_ratio_with_custom_risk_free_rate(self, sample_equity_curve):
        """Test Sharpe ratio with custom risk-free rate."""
        sharpe_no_rf = calculate_sharpe_ratio(sample_equity_curve, risk_free_rate=0.0)
        sharpe_with_rf = calculate_sharpe_ratio(
            sample_equity_curve, risk_free_rate=0.02
        )

        # With positive risk-free rate, Sharpe should generally be lower
        assert isinstance(sharpe_no_rf, float)
        assert isinstance(sharpe_with_rf, float)

    def test_sharpe_ratio_with_custom_periods(self, sample_equity_curve):
        """Test Sharpe ratio with different annualization periods."""
        sharpe_daily = calculate_sharpe_ratio(sample_equity_curve, periods_per_year=252)
        sharpe_weekly = calculate_sharpe_ratio(sample_equity_curve, periods_per_year=52)

        assert isinstance(sharpe_daily, float)
        assert isinstance(sharpe_weekly, float)

    def test_metrics_handle_edge_cases(self, base_timestamp):
        """Test that metrics handle edge cases gracefully."""
        # Single data point
        single_point = [(base_timestamp, 100000.0)]

        sharpe = calculate_sharpe_ratio(single_point)
        assert sharpe == 0.0

        sortino = calculate_sortino_ratio(single_point)
        assert sortino == 0.0

        calmar = calculate_calmar_ratio(single_point)
        assert calmar == 0.0
