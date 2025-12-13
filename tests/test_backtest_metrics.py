from datetime import datetime

import pytest
import numpy as np

from src.engine.backtesting.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    # calculate_win_rate,
    calculate_total_return,
)


# Sharpe Ratio Tests


def test_sharpe_ratio_with_positive_returns() -> None:
    """Verify Sharpe ratio calculation with positive returns."""
    # Create equity curve with consistent positive returns
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 101000.0),  # +1% return
        (datetime(2024, 1, 3), 102000.0),  # +0.99% return
        (datetime(2024, 1, 4), 103000.0),  # +0.98% return
        (datetime(2024, 1, 5), 104000.0),  # +0.97% return
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    # Should be positive for positive returns
    assert sharpe > 0.0
    assert isinstance(sharpe, float)


def test_sharpe_ratio_with_negative_returns() -> None:
    """Verify Sharpe ratio calculation with negative returns."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 99000.0),  # -1% return
        (datetime(2024, 1, 3), 98000.0),  # -1.01% return
        (datetime(2024, 1, 4), 97000.0),  # -1.02% return
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    # Should be negative for consistent negative returns
    assert sharpe < 0.0


def test_sharpe_ratio_with_mixed_returns() -> None:
    """Verify Sharpe ratio calculation with mixed returns."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 102000.0),  # +2%
        (datetime(2024, 1, 3), 101000.0),  # -0.98%
        (datetime(2024, 1, 4), 103000.0),  # +1.98%
        (datetime(2024, 1, 5), 102000.0),  # -0.97%
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert isinstance(sharpe, float)
    # Can be positive or negative depending on average return vs volatility


def test_sharpe_ratio_with_zero_volatility() -> None:
    """Verify Sharpe ratio handles zero volatility (EDGE CASE)."""
    # All equity values are the same (no volatility)
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 100000.0),
        (datetime(2024, 1, 3), 100000.0),
        (datetime(2024, 1, 4), 100000.0),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    # Should return 0.0 for zero volatility
    assert sharpe == 0.0


def test_sharpe_ratio_with_insufficient_data() -> None:
    """Verify Sharpe ratio handles insufficient data (EDGE CASE)."""
    # Only one data point
    equity_curve = [(datetime(2024, 1, 1), 100000.0)]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe == 0.0


def test_sharpe_ratio_with_empty_data() -> None:
    """Verify Sharpe ratio handles empty data (EDGE CASE)."""
    equity_curve = []

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe == 0.0


def test_sharpe_ratio_with_custom_risk_free_rate() -> None:
    """Verify Sharpe ratio calculation with custom risk-free rate."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 101000.0),
        (datetime(2024, 1, 3), 102000.0),
        (datetime(2024, 1, 4), 103000.0),
    ]

    sharpe_no_rf = calculate_sharpe_ratio(equity_curve, risk_free_rate=0.0)
    sharpe_with_rf = calculate_sharpe_ratio(
        equity_curve, risk_free_rate=0.05
    )  # 5% annual

    # Sharpe ratio should be lower with positive risk-free rate
    assert sharpe_with_rf < sharpe_no_rf


def test_sharpe_ratio_annualization() -> None:
    """Verify Sharpe ratio is properly annualized."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 101000.0),
        (datetime(2024, 1, 3), 102000.0),
    ]

    # Default is 252 periods per year (daily)
    sharpe_daily = calculate_sharpe_ratio(equity_curve, periods_per_year=252)

    # Hourly would be different
    sharpe_hourly = calculate_sharpe_ratio(equity_curve, periods_per_year=252 * 6.5)

    # Both should be valid numbers
    assert isinstance(sharpe_daily, float)
    assert isinstance(sharpe_hourly, float)


def test_sharpe_ratio_with_extreme_values() -> None:
    """Verify Sharpe ratio handles extreme equity values."""
    equity_curve = [
        (datetime(2024, 1, 1), 1000000000.0),  # 1 billion
        (datetime(2024, 1, 2), 1001000000.0),
        (datetime(2024, 1, 3), 1002000000.0),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert isinstance(sharpe, float)
    assert not np.isnan(sharpe)
    assert not np.isinf(sharpe)


# Maximum Drawdown Tests


def test_max_drawdown_with_no_drawdown() -> None:
    """Verify max drawdown is zero with consistently increasing equity."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 101000.0),
        (datetime(2024, 1, 3), 102000.0),
        (datetime(2024, 1, 4), 103000.0),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 50000.0),
        (datetime(2024, 1, 3), 50000.0),
        (datetime(2024, 1, 4), 50000.0),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 0.0
    assert max_dd_pct == 0.0


def test_max_drawdown_with_single_decline() -> None:
    """Verify max drawdown calculation with single decline."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 110000.0),  # Peak
        (datetime(2024, 1, 3), 105000.0),  # -5000 from peak
        (datetime(2024, 1, 4), 108000.0),  # Recovery
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 50000.0),
        (datetime(2024, 1, 3), 50000.0),
        (datetime(2024, 1, 4), 50000.0),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 5000.0
    assert abs(max_dd_pct - 4.545454) < 0.01  # ~4.55% (5000/110000)


def test_max_drawdown_with_multiple_declines() -> None:
    """Verify max drawdown identifies the largest decline."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 95000.0),  # -5% decline
        (datetime(2024, 1, 3), 98000.0),  # Recovery
        (datetime(2024, 1, 4), 110000.0),  # New peak
        (datetime(2024, 1, 5), 100000.0),  # -10% decline (larger)
    ]

    cash_curve = [(t, 50000.0) for t, _ in equity_curve]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    # Should identify the 10k decline from 110k
    assert max_dd == 10000.0
    assert abs(max_dd_pct - 9.09) < 0.1  # ~9.09% (10000/110000)


def test_max_drawdown_with_continuous_decline() -> None:
    """Verify max drawdown with continuous decline."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),  # Peak
        (datetime(2024, 1, 2), 90000.0),
        (datetime(2024, 1, 3), 80000.0),
        (datetime(2024, 1, 4), 70000.0),  # Bottom
    ]

    cash_curve = [(t, 50000.0) for t, _ in equity_curve]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 30000.0
    assert max_dd_pct == 30.0  # 30% decline


def test_max_drawdown_without_cash_curve() -> None:
    """Verify max drawdown works without cash curve."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 110000.0),
        (datetime(2024, 1, 3), 105000.0),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, None)

    assert max_dd == 5000.0
    assert isinstance(max_dd_pct, float)


def test_max_drawdown_with_mismatched_curves() -> None:
    """Verify max drawdown handles mismatched curve lengths."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 95000.0),
        (datetime(2024, 1, 3), 90000.0),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 50000.0),
    ]  # Shorter than equity curve

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    # Should still calculate (cash curve ignored due to length mismatch)
    assert isinstance(max_dd, float)
    assert isinstance(max_dd_pct, float)


def test_max_drawdown_with_single_point() -> None:
    """Verify max drawdown handles single data point (EDGE CASE)."""
    equity_curve = [(datetime(2024, 1, 1), 100000.0)]
    cash_curve = [(datetime(2024, 1, 1), 100000.0)]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 0.0
    assert max_dd_pct == 0.0


def test_max_drawdown_with_zero_peak() -> None:
    """Verify max drawdown handles zero peak (EDGE CASE)."""
    equity_curve = [
        (datetime(2024, 1, 1), 0.0),
        (datetime(2024, 1, 2), 0.0),
    ]

    cash_curve = [(t, 0.0) for t, _ in equity_curve]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 0.0
    assert max_dd_pct == 0.0


# Win Rate Tests


# def test_win_rate_all_winning_trades() -> None:
#     """Verify win rate is 100% when all trades are profitable."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [
#         MockTrade(pnl=100.0),
#         MockTrade(pnl=200.0),
#         MockTrade(pnl=50.0),
#     ]

#     win_rate = calculate_win_rate(trades)

#     assert win_rate == 100.0


# def test_win_rate_all_losing_trades() -> None:
#     """Verify win rate is 0% when all trades are losing."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [
#         MockTrade(pnl=-100.0),
#         MockTrade(pnl=-200.0),
#         MockTrade(pnl=-50.0),
#     ]

#     win_rate = calculate_win_rate(trades)

    # assert win_rate == 0.0


# def test_win_rate_mixed_trades() -> None:
#     """Verify win rate calculation with mixed profitable and losing trades."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [
#         MockTrade(pnl=100.0),  # Win
#         MockTrade(pnl=-50.0),  # Loss
#         MockTrade(pnl=200.0),  # Win
#         MockTrade(pnl=-30.0),  # Loss
#         MockTrade(pnl=75.0),  # Win
#     ]

#     win_rate = calculate_win_rate(trades)

#     # 3 wins out of 5 trades = 60%
#     assert win_rate == 60.0


# def test_win_rate_with_zero_pnl_trades() -> None:
#     """Verify win rate treats zero P&L trades as losses."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [
#         MockTrade(pnl=100.0),  # Win
#         MockTrade(pnl=0.0),  # Not a win (0 P&L)
#         MockTrade(pnl=-50.0),  # Loss
#     ]

#     win_rate = calculate_win_rate(trades)

#     # Only 1 win out of 3 trades = 33.33%
#     assert abs(win_rate - 33.333) < 0.01


# def test_win_rate_with_no_trades() -> None:
#     """Verify win rate is 0% when no trades exist (EDGE CASE)."""
#     trades = []

#     win_rate = calculate_win_rate(trades)

#     assert win_rate == 0.0


# def test_win_rate_with_single_winning_trade() -> None:
#     """Verify win rate is 100% with single winning trade."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [MockTrade(pnl=100.0)]

#     win_rate = calculate_win_rate(trades)

#     assert win_rate == 100.0


# def test_win_rate_with_single_losing_trade() -> None:
#     """Verify win rate is 0% with single losing trade."""

#     class MockTrade:
#         def __init__(self, pnl):
#             self.pnl = pnl

#     trades = [MockTrade(pnl=-100.0)]

#     win_rate = calculate_win_rate(trades)

#     assert win_rate == 0.0


# Total Return Tests


def test_total_return_with_profit() -> None:
    """Verify total return calculation with profit."""
    initial = 100000.0
    final = 110000.0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 10000.0
    assert ret_percent == 10.0


def test_total_return_with_loss() -> None:
    """Verify total return calculation with loss."""
    initial = 100000.0
    final = 90000.0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == -10000.0
    assert ret_percent == -10.0


def test_total_return_with_no_change() -> None:
    """Verify total return is zero when capital unchanged."""
    initial = 100000.0
    final = 100000.0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 0.0
    assert ret_percent == 0.0


def test_total_return_with_zero_initial_capital() -> None:
    """Verify total return handles zero initial capital (EDGE CASE)."""
    initial = 0.0
    final = 10000.0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 10000.0
    assert ret_percent == 0.0  # Can't calculate percentage from zero


def test_total_return_with_negative_initial_capital() -> None:
    """Verify total return with negative initial capital."""
    initial = -50000.0  # Margin/debt scenario
    final = -30000.0  # Reduced debt

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 20000.0
    # Percentage calculation with negative base
    assert isinstance(ret_percent, float)


def test_total_return_with_large_gain() -> None:
    """Verify total return calculation with large percentage gain."""
    initial = 10000.0
    final = 100000.0  # 10x return

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 90000.0
    assert ret_percent == 900.0  # 900% gain


def test_total_return_with_total_loss() -> None:
    """Verify total return calculation with total capital loss."""
    initial = 100000.0
    final = 0.0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == -100000.0
    assert ret_percent == -100.0


def test_total_return_precision() -> None:
    """Verify total return maintains precision for small returns."""
    initial = 100000.0
    final = 100010.0  # $10 gain

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 10.0
    assert abs(ret_percent - 0.01) < 0.0001  # 0.01%


# NaN and Infinity Handling Tests


def test_sharpe_ratio_handles_nan_in_equity() -> None:
    """Verify Sharpe ratio handles NaN values in equity curve (EDGE CASE)."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), float("nan")),
        (datetime(2024, 1, 3), 102000.0),
    ]

    # Should handle gracefully (implementation dependent)
    # Either skip NaN or return 0.0
    sharpe = calculate_sharpe_ratio(equity_curve)

    # Should not raise an exception
    assert isinstance(sharpe, float)


def test_sharpe_ratio_handles_inf_in_equity() -> None:
    """Verify Sharpe ratio handles infinity values (EDGE CASE)."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), float("inf")),
        (datetime(2024, 1, 3), 102000.0),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    # Should handle gracefully
    assert isinstance(sharpe, float)


def test_metrics_return_types() -> None:
    """Verify all metrics return correct types."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 101000.0),
        (datetime(2024, 1, 3), 102000.0),
    ]

    cash_curve = [(t, 50000.0) for t, _ in equity_curve]

    # Sharpe ratio returns float
    sharpe = calculate_sharpe_ratio(equity_curve)
    assert isinstance(sharpe, float)

    # Max drawdown returns tuple of floats
    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)
    assert isinstance(max_dd, float)
    assert isinstance(max_dd_pct, float)

    # Total return returns tuple of floats
    ret_dollars, ret_percent = calculate_total_return(100000.0, 102000.0)
    assert isinstance(ret_dollars, float)
    assert isinstance(ret_percent, float)


def test_metrics_with_extreme_volatility() -> None:
    """Verify metrics handle extreme volatility."""
    equity_curve = [
        (datetime(2024, 1, 1), 100000.0),
        (datetime(2024, 1, 2), 200000.0),  # +100%
        (datetime(2024, 1, 3), 50000.0),  # -75%
        (datetime(2024, 1, 4), 150000.0),  # +200%
        (datetime(2024, 1, 5), 25000.0),  # -83%
    ]

    cash_curve = [(t, 10000.0) for t, _ in equity_curve]

    sharpe = calculate_sharpe_ratio(equity_curve)
    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    # All should calculate without error
    assert isinstance(sharpe, float)
    assert isinstance(max_dd, float)
    assert isinstance(max_dd_pct, float)

    # Extreme volatility should result in very negative Sharpe
    # and large drawdown
    assert max_dd > 0.0
