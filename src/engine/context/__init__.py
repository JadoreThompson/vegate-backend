"""
Context module for trading strategies.

This module provides the runtime context that strategies use to access market data,
execute trades, and calculate technical indicators. It exports the base StrategyContext
class and the IndicatorMixin for enhanced functionality.
"""

from .base import StrategyContext, HistoricalData
from .indicators import IndicatorMixin

__all__ = [
    "StrategyContext",
    "HistoricalData",
    "IndicatorMixin",
]
