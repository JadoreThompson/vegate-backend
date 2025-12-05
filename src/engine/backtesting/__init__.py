"""
Backtesting module for trading strategy framework.

This module provides a complete backtesting engine that simulates strategy execution
over historical OHLC data with realistic order fills, slippage, and commission modeling.

Main Components:
    - OHLCDataLoader: Loads historical data from PostgreSQL
    - SimulatedBroker: Simulates order execution with realistic fills
    - BacktestEngine: Orchestrates the complete backtest flow
    - Metrics: Performance calculation functions
    - BacktestConfig: Configuration for backtest parameters
    - BacktestResult: Comprehensive backtest results

Example:
    from engine.backtesting import (
        BacktestEngine,
        BacktestConfig,
        OHLCDataLoader,
        Timeframe
    )
    from datetime import datetime
    from utils.db import get_db_sess

    # Define strategy
    def my_strategy(ctx):
        price = ctx.close("AAPL")
        if price > 150:
            ctx.buy("AAPL", quantity=10)

    # Configure backtest
    config = BacktestConfig(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 12, 31),
        symbols=["AAPL"],
        initial_capital=100000.0,
        timeframe=Timeframe.MINUTE_1
    )

    # Run backtest
    with get_db_sess() as session:
        loader = OHLCDataLoader(session)
        engine = BacktestEngine(config, loader, my_strategy)
        result = engine.run()

        print(f"Total Return: {result.total_return_percent:.2f}%")
        print(f"Sharpe Ratio: {result.sharpe_ratio:.2f}")
"""

from .data_loader import OHLCDataLoader, OHLCBar, Timeframe, TradeRecord

from .simulated_broker import SimulatedBroker

from .engine import BacktestEngine, BacktestConfig, BacktestResult, BacktestContext

from .metrics import (
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

__all__ = [
    # Data Loading
    "OHLCDataLoader",
    "OHLCBar",
    "Timeframe",
    "TradeRecord",
    # Broker
    "SimulatedBroker",
    # Engine
    "BacktestEngine",
    "BacktestConfig",
    "BacktestResult",
    "BacktestContext",
    # Metrics
    "calculate_sharpe_ratio",
    "calculate_max_drawdown",
    "calculate_win_rate",
    "calculate_total_return",
    "calculate_average_trade",
    "calculate_profit_factor",
    "calculate_sortino_ratio",
    "calculate_calmar_ratio",
    "calculate_recovery_factor",
    "calculate_equity_curve_stats",
    "calculate_trade_stats",
]
