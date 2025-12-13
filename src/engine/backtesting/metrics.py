import logging
import numpy as np

from .types import EquityCurve


logger = logging.getLogger(__name__)


def calculate_sharpe_ratio(
    equity_curve: EquityCurve, risk_free_rate: float = 0.0, periods_per_year: int = 252
) -> float:
    """
    Calculate the Sharpe ratio from an equity curve.

    The Sharpe ratio measures risk-adjusted return by comparing excess returns
    to volatility. Higher values indicate better risk-adjusted performance.

    Args:
        equity_curve: list of (timestamp, equity) tuples
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
        # Extract equity values and convert Decimal to float
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
    equity_curve: EquityCurve,
    cash_curve: EquityCurve,
) -> tuple[float, float]:
    """
    Calculate maximum drawdown from an equity curve.

    Drawdown is the peak-to-trough decline in equity. Maximum drawdown
    represents the largest loss from a peak. When cash_curve is provided,
    it is used to understand the actual cash balance at each point in time.

    Args:
        equity_curve: list of (timestamp, equity) tuples
        cash_curve: Optional list of (timestamp, cash) tuples for cash balance tracking

    Returns:
        tuple of (max_drawdown_dollars, max_drawdown_percent)

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

    try:
        # Extract equity values and convert Decimal to float
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

        return max_dd, max_dd_pct

    except Exception as e:
        logger.error(f"Error calculating max drawdown: {e}", exc_info=True)
        return 0.0, 0.0


def calculate_total_return(
    initial_capital: float, final_capital: float
) -> tuple[float, float]:
    """
    Calculate total return from initial and final capital.

    Args:
        initial_capital: Starting capital
        final_capital: Ending capital

    Returns:
        tuple of (return_dollars, return_percent)

    Example:
        ret_dollars, ret_percent = calculate_total_return(100000, 110000)
    """
    try:
        return_dollars = final_capital - initial_capital
        return_percent = (
            (return_dollars / initial_capital * 100) if initial_capital > 0 else 0.0
        )

        return return_dollars, return_percent

    except Exception as e:
        logger.error(f"Error calculating total return: {e}", exc_info=True)
        return 0.0, 0.0
