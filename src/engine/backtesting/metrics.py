import logging
from typing import List, Tuple, Optional
from datetime import datetime
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_sharpe_ratio(
    equity_curve: List[Tuple[datetime, float]],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """
    Calculate the Sharpe ratio from an equity curve.

    The Sharpe ratio measures risk-adjusted return by comparing excess returns
    to volatility. Higher values indicate better risk-adjusted performance.

    Args:
        equity_curve: List of (timestamp, equity) tuples
        risk_free_rate: Annual risk-free rate (default: 0.0)
        periods_per_year: Number of periods per year for annualization (default: 252 for daily)

    Returns:
        Annualized Sharpe ratio

    Example:
        equity_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 101000),
            (datetime(2024, 1, 3), 100500)
        ]
        sharpe = calculate_sharpe_ratio(equity_curve)
    """
    if len(equity_curve) < 2:
        logger.warning("Insufficient data for Sharpe ratio calculation")
        return 0.0

    try:
        # Extract equity values
        equity_values = [equity for _, equity in equity_curve]

        # Calculate period returns
        returns = []
        for i in range(1, len(equity_values)):
            ret = (equity_values[i] - equity_values[i - 1]) / equity_values[i - 1]
            returns.append(ret)

        if not returns:
            return 0.0

        returns_array = np.array(returns)

        # Calculate mean and std of returns
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array, ddof=1)  # Sample std

        if std_return == 0 or np.isnan(std_return):
            logger.warning("Zero or invalid standard deviation in returns")
            return 0.0

        # Calculate daily risk-free rate
        daily_rf = risk_free_rate / periods_per_year

        # Calculate Sharpe ratio and annualize
        sharpe = ((mean_return - daily_rf) / std_return) * np.sqrt(periods_per_year)

        return float(sharpe)

    except Exception as e:
        logger.error(f"Error calculating Sharpe ratio: {e}", exc_info=True)
        return 0.0


def calculate_max_drawdown(
    equity_curve: List[Tuple[datetime, float]],
    cash_curve: Optional[List[Tuple[datetime, float]]] = None,
) -> Tuple[float, float]:
    """
    Calculate maximum drawdown from an equity curve.

    Drawdown is the peak-to-trough decline in equity. Maximum drawdown
    represents the largest loss from a peak. When cash_curve is provided,
    it is used to understand the actual cash balance at each point in time.

    Args:
        equity_curve: List of (timestamp, equity) tuples
        cash_curve: Optional list of (timestamp, cash) tuples for cash balance tracking

    Returns:
        Tuple of (max_drawdown_dollars, max_drawdown_percent)

    Example:
        equity_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 95000),
            (datetime(2024, 1, 3), 98000)
        ]
        cash_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 50000),
            (datetime(2024, 1, 3), 50000)
        ]
        dd_dollars, dd_percent = calculate_max_drawdown(equity_curve, cash_curve)
    """
    if not equity_curve:
        logger.warning("Empty equity curve for drawdown calculation")
        return 0.0, 0.0

    try:
        equity_values = [equity for _, equity in equity_curve]

        # If cash curve is provided, use it for additional context
        # This allows tracking actual cash balance at each point in time
        cash_values = None
        if cash_curve:
            cash_values = [cash for _, cash in cash_curve]
            if len(cash_values) != len(equity_values):
                logger.warning(
                    "Cash curve length doesn't match equity curve, ignoring cash data"
                )
                cash_values = None

        peak = equity_values[0]
        max_dd = 0.0
        max_dd_pct = 0.0

        for i, equity in enumerate(equity_values):
            # Update peak
            if equity > peak:
                peak = equity

            # Calculate drawdown from peak
            dd = peak - equity
            dd_pct = (dd / peak * 100) if peak > 0 else 0.0

            # Log cash balance at drawdown points if available
            if cash_values and dd > 0:
                cash_at_time = cash_values[i]
                logger.debug(
                    f"Drawdown: ${dd:.2f} ({dd_pct:.2f}%), Cash: ${cash_at_time:.2f}"
                )

            # Update max drawdown
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct

        return float(max_dd), float(max_dd_pct)

    except Exception as e:
        logger.error(f"Error calculating max drawdown: {e}", exc_info=True)
        return 0.0, 0.0


def calculate_win_rate(trades: List) -> float:
    """
    Calculate win rate from trade history.

    Win rate is the percentage of profitable trades.

    Args:
        trades: List of TradeRecord objects with pnl attribute

    Returns:
        Win rate as percentage (0-100)

    Example:
        trades = [trade1, trade2, trade3]  # TradeRecord objects
        win_rate = calculate_win_rate(trades)
    """
    if not trades:
        logger.warning("No trades for win rate calculation")
        return 0.0

    try:
        winning_trades = sum(1 for trade in trades if trade.pnl > 0)
        total_trades = len(trades)

        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

        return float(win_rate)

    except Exception as e:
        logger.error(f"Error calculating win rate: {e}", exc_info=True)
        return 0.0


def calculate_total_return(
    initial_capital: float, final_capital: float
) -> Tuple[float, float]:
    """
    Calculate total return from initial and final capital.

    Args:
        initial_capital: Starting capital
        final_capital: Ending capital

    Returns:
        Tuple of (return_dollars, return_percent)

    Example:
        ret_dollars, ret_percent = calculate_total_return(100000, 110000)
    """
    try:
        return_dollars = final_capital - initial_capital
        return_percent = (
            (return_dollars / initial_capital * 100) if initial_capital > 0 else 0.0
        )

        return float(return_dollars), float(return_percent)

    except Exception as e:
        logger.error(f"Error calculating total return: {e}", exc_info=True)
        return 0.0, 0.0


def calculate_average_trade(trades: List) -> Tuple[float, float, float]:
    """
    Calculate average trade statistics.

    Args:
        trades: List of TradeRecord objects

    Returns:
        Tuple of (avg_pnl, avg_win, avg_loss)

    Example:
        avg_pnl, avg_win, avg_loss = calculate_average_trade(trades)
    """
    if not trades:
        return 0.0, 0.0, 0.0

    try:
        winning_trades = [t.pnl for t in trades if t.pnl > 0]
        losing_trades = [t.pnl for t in trades if t.pnl < 0]

        avg_pnl = np.mean([t.pnl for t in trades]) if trades else 0.0
        avg_win = np.mean(winning_trades) if winning_trades else 0.0
        avg_loss = np.mean(losing_trades) if losing_trades else 0.0

        return float(avg_pnl), float(avg_win), float(avg_loss)

    except Exception as e:
        logger.error(f"Error calculating average trade: {e}", exc_info=True)
        return 0.0, 0.0, 0.0


def calculate_profit_factor(trades: List) -> float:
    """
    Calculate profit factor (gross profit / gross loss).

    A profit factor > 1 indicates profitability.

    Args:
        trades: List of TradeRecord objects

    Returns:
        Profit factor

    Example:
        pf = calculate_profit_factor(trades)
    """
    if not trades:
        return 0.0

    try:
        gross_profit = sum(t.pnl for t in trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))

        if gross_loss == 0:
            return float("inf") if gross_profit > 0 else 0.0

        profit_factor = gross_profit / gross_loss

        return float(profit_factor)

    except Exception as e:
        logger.error(f"Error calculating profit factor: {e}", exc_info=True)
        return 0.0


def calculate_sortino_ratio(
    equity_curve: List[Tuple[datetime, float]],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """
    Calculate Sortino ratio (similar to Sharpe but uses downside deviation).

    The Sortino ratio only considers downside volatility, making it more
    appropriate for strategies with asymmetric return distributions.

    Args:
        equity_curve: List of (timestamp, equity) tuples
        risk_free_rate: Annual risk-free rate (default: 0.0)
        periods_per_year: Number of periods per year (default: 252)

    Returns:
        Annualized Sortino ratio
    """
    if len(equity_curve) < 2:
        return 0.0

    try:
        equity_values = [equity for _, equity in equity_curve]

        returns = []
        for i in range(1, len(equity_values)):
            ret = (equity_values[i] - equity_values[i - 1]) / equity_values[i - 1]
            returns.append(ret)

        if not returns:
            return 0.0

        returns_array = np.array(returns)
        mean_return = np.mean(returns_array)

        # Calculate downside deviation (only negative returns)
        downside_returns = returns_array[returns_array < 0]

        if len(downside_returns) == 0:
            return float("inf") if mean_return > 0 else 0.0

        downside_std = np.std(downside_returns, ddof=1)

        if downside_std == 0:
            return 0.0

        daily_rf = risk_free_rate / periods_per_year
        sortino = ((mean_return - daily_rf) / downside_std) * np.sqrt(periods_per_year)

        return float(sortino)

    except Exception as e:
        logger.error(f"Error calculating Sortino ratio: {e}", exc_info=True)
        return 0.0


def calculate_calmar_ratio(
    equity_curve: List[Tuple[datetime, float]], periods_per_year: int = 252
) -> float:
    """
    Calculate Calmar ratio (annualized return / max drawdown).

    The Calmar ratio measures return relative to maximum drawdown,
    useful for evaluating downside risk.

    Args:
        equity_curve: List of (timestamp, equity) tuples
        periods_per_year: Number of periods per year (default: 252)

    Returns:
        Calmar ratio
    """
    if len(equity_curve) < 2:
        return 0.0

    try:
        initial_equity = equity_curve[0][1]
        final_equity = equity_curve[-1][1]

        # Calculate total return
        total_return = (final_equity - initial_equity) / initial_equity

        # Estimate number of periods and annualize
        num_periods = len(equity_curve) - 1
        years = num_periods / periods_per_year

        if years <= 0:
            return 0.0

        annualized_return = (1 + total_return) ** (1 / years) - 1

        # Calculate max drawdown
        _, max_dd_pct = calculate_max_drawdown(equity_curve)

        if max_dd_pct == 0:
            return float("inf") if annualized_return > 0 else 0.0

        calmar = (annualized_return * 100) / max_dd_pct

        return float(calmar)

    except Exception as e:
        logger.error(f"Error calculating Calmar ratio: {e}", exc_info=True)
        return 0.0


def calculate_recovery_factor(equity_curve: List[Tuple[datetime, float]]) -> float:
    """
    Calculate recovery factor (net profit / max drawdown).

    Measures how well the strategy recovers from drawdowns.

    Args:
        equity_curve: List of (timestamp, equity) tuples

    Returns:
        Recovery factor
    """
    if len(equity_curve) < 2:
        return 0.0

    try:
        initial_equity = equity_curve[0][1]
        final_equity = equity_curve[-1][1]
        net_profit = final_equity - initial_equity

        max_dd, _ = calculate_max_drawdown(equity_curve)

        if max_dd == 0:
            return float("inf") if net_profit > 0 else 0.0

        recovery = net_profit / max_dd

        return float(recovery)

    except Exception as e:
        logger.error(f"Error calculating recovery factor: {e}", exc_info=True)
        return 0.0


def calculate_equity_curve_stats(equity_curve: List[Tuple[datetime, float]]) -> dict:
    """
    Calculate comprehensive statistics from equity curve.

    Args:
        equity_curve: List of (timestamp, equity) tuples

    Returns:
        Dictionary with various statistics
    """
    if not equity_curve:
        return {}

    try:
        equity_values = [equity for _, equity in equity_curve]

        return {
            "min_equity": float(np.min(equity_values)),
            "max_equity": float(np.max(equity_values)),
            "mean_equity": float(np.mean(equity_values)),
            "std_equity": float(np.std(equity_values)),
            "sharpe_ratio": calculate_sharpe_ratio(equity_curve),
            "sortino_ratio": calculate_sortino_ratio(equity_curve),
            "calmar_ratio": calculate_calmar_ratio(equity_curve),
            "recovery_factor": calculate_recovery_factor(equity_curve),
        }

    except Exception as e:
        logger.error(f"Error calculating equity curve stats: {e}", exc_info=True)
        return {}


def calculate_trade_stats(trades: List) -> dict:
    """
    Calculate comprehensive trade statistics.

    Args:
        trades: List of TradeRecord objects

    Returns:
        Dictionary with various trade statistics
    """
    if not trades:
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
            "total_pnl": 0.0,
            "total_commission": 0.0,
            "total_slippage": 0.0,
        }

    try:
        winning_trades = [t for t in trades if t.pnl > 0]
        losing_trades = [t for t in trades if t.pnl < 0]

        avg_pnl, avg_win, avg_loss = calculate_average_trade(trades)

        return {
            "total_trades": len(trades),
            "winning_trades": len(winning_trades),
            "losing_trades": len(losing_trades),
            "win_rate": calculate_win_rate(trades),
            "avg_pnl": avg_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "profit_factor": calculate_profit_factor(trades),
            "total_pnl": sum(t.pnl for t in trades),
            "total_commission": sum(t.commission for t in trades),
            "total_slippage": sum(t.slippage for t in trades),
        }

    except Exception as e:
        logger.error(f"Error calculating trade stats: {e}", exc_info=True)
        return {}
