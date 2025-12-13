import pytest
from datetime import datetime

from engine.backtesting.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_total_return,
)


def test_calculate_sharpe_ratio_with_positive_returns():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 101000),
        (datetime(2024, 1, 3), 102000),
        (datetime(2024, 1, 4), 103000),
        (datetime(2024, 1, 5), 104000),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe > 0
    assert isinstance(sharpe, float)


def test_calculate_sharpe_ratio_with_negative_returns():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 99000),
        (datetime(2024, 1, 3), 98000),
        (datetime(2024, 1, 4), 97000),
        (datetime(2024, 1, 5), 96000),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe < 0
    assert isinstance(sharpe, float)


def test_calculate_sharpe_ratio_with_volatile_returns():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 105000),
        (datetime(2024, 1, 3), 98000),
        (datetime(2024, 1, 4), 110000),
        (datetime(2024, 1, 5), 95000),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert isinstance(sharpe, float)


def test_calculate_sharpe_ratio_with_flat_returns():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 100000),
        (datetime(2024, 1, 3), 100000),
        (datetime(2024, 1, 4), 100000),
    ]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe == 0.0


def test_calculate_sharpe_ratio_with_insufficient_data():
    equity_curve = [(datetime(2024, 1, 1), 100000)]

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe == 0.0


def test_calculate_sharpe_ratio_with_empty_curve():
    equity_curve = []

    sharpe = calculate_sharpe_ratio(equity_curve)

    assert sharpe == 0.0


def test_calculate_sharpe_ratio_with_custom_risk_free_rate():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 101000),
        (datetime(2024, 1, 3), 102000),
    ]

    sharpe_no_rf = calculate_sharpe_ratio(equity_curve, risk_free_rate=0.0)
    sharpe_with_rf = calculate_sharpe_ratio(equity_curve, risk_free_rate=0.02)

    assert sharpe_with_rf < sharpe_no_rf


def test_calculate_max_drawdown_with_drawdown():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 110000),
        (datetime(2024, 1, 3), 105000),
        (datetime(2024, 1, 4), 95000),
        (datetime(2024, 1, 5), 100000),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 50000),
        (datetime(2024, 1, 2), 50000),
        (datetime(2024, 1, 3), 50000),
        (datetime(2024, 1, 4), 50000),
        (datetime(2024, 1, 5), 50000),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == -15000
    assert max_dd_pct == pytest.approx(-13.64, rel=0.1)


def test_calculate_max_drawdown_no_drawdown():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 101000),
        (datetime(2024, 1, 3), 102000),
        (datetime(2024, 1, 4), 103000),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 101000),
        (datetime(2024, 1, 3), 102000),
        (datetime(2024, 1, 4), 103000),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == 0.0
    assert max_dd_pct == 0.0


def test_calculate_max_drawdown_without_cash_curve():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 110000),
        (datetime(2024, 1, 3), 95000),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, None)

    assert max_dd == -15000
    assert max_dd_pct == pytest.approx(-13.64, rel=0.1)


def test_calculate_max_drawdown_multiple_peaks():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 110000),
        (datetime(2024, 1, 3), 105000),
        (datetime(2024, 1, 4), 115000),
        (datetime(2024, 1, 5), 100000),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 50000),
        (datetime(2024, 1, 2), 50000),
        (datetime(2024, 1, 3), 50000),
        (datetime(2024, 1, 4), 50000),
        (datetime(2024, 1, 5), 50000),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert max_dd == -15000
    assert max_dd_pct == pytest.approx(-13.04, rel=0.1)


def test_calculate_max_drawdown_mismatched_curves():
    equity_curve = [
        (datetime(2024, 1, 1), 100000),
        (datetime(2024, 1, 2), 110000),
    ]

    cash_curve = [
        (datetime(2024, 1, 1), 50000),
    ]

    max_dd, max_dd_pct = calculate_max_drawdown(equity_curve, cash_curve)

    assert isinstance(max_dd, float)
    assert isinstance(max_dd_pct, float)


def test_calculate_total_return_profit():
    initial = 100000
    final = 110000

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 10000
    assert ret_percent == 10.0


def test_calculate_total_return_loss():
    initial = 100000
    final = 95000

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == -5000
    assert ret_percent == -5.0


def test_calculate_total_return_no_change():
    initial = 100000
    final = 100000

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 0
    assert ret_percent == 0.0


def test_calculate_total_return_zero_initial():
    initial = 0
    final = 10000

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 10000
    assert ret_percent == 0.0


def test_calculate_total_return_large_profit():
    initial = 100000
    final = 500000

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == 400000
    assert ret_percent == 400.0


def test_calculate_total_return_total_loss():
    initial = 100000
    final = 0

    ret_dollars, ret_percent = calculate_total_return(initial, final)

    assert ret_dollars == -100000
    assert ret_percent == -100.0
