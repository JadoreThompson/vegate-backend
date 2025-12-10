import logging
from datetime import date
from decimal import Decimal

from pydantic import field_validator

from core.models import CustomBaseModel
from engine.brokers import BacktestBroker
from engine.enums import Timeframe
from engine.models import OrderResponse
from engine.strategy import BaseStrategy, StrategyContext, StrategyManager
from .metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_total_return,
)
from .types import EquityCurveT


logger = logging.getLogger(__name__)


class BacktestConfig(CustomBaseModel):
    start_date: date
    end_date: date
    symbol: str
    starting_balance: float | Decimal = 100_000.0
    timeframe: Timeframe


class BacktestResult(CustomBaseModel):
    config: BacktestConfig
    realised_pnl: float
    unrealised_pnl: float
    total_return_pct: float
    sharpe_ratio: float
    max_drawdown: float
    equity_curve: EquityCurveT

    @field_validator(
        "realised_pnl",
        "unrealised_pnl",
        "total_return_pct",
        "sharpe_ratio",
        "max_drawdown",
        mode="after",
    )
    def round_values(cls, value):
        return round(value, 2)


class SpotBacktestResult(BacktestResult):
    orders: list[OrderResponse]
    total_orders: int


class BacktestEngine:
    def __init__(self, strategy: BaseStrategy, config: BacktestConfig):
        self._config = config
        self._strategy = strategy
        self._broker = BacktestBroker(starting_balance=self._config.starting_balance)
        self._strategy_manager = StrategyManager(self._strategy, self._broker)
        self._strategy_context = StrategyContext[BacktestBroker](self._broker)

        self._equity_curve: EquityCurveT = []
        self._cash_balance_curve: EquityCurveT = []

    def run(self) -> SpotBacktestResult:
        for ohlcv in self._broker.yield_historic_ohlcv(
            self._config.symbol,
            self._config.timeframe,
            start_date=self._config.start_date,
            end_date=self._config.end_date,
        ):
            account = self._broker.get_account()
            self._equity_curve.append((ohlcv.timestamp, account.equity))
            self._cash_balance_curve.append((ohlcv.timestamp, account.cash))

            self._strategy_context._current_candle = ohlcv
            self._broker.process_pending_orders()
            self._strategy_manager.on_candle(self._strategy_context)

        return self._calculate_results()

    def _calculate_results(self) -> SpotBacktestResult:
        """
        Calculate comprehensive performance metrics.

        Returns:
            SpotBacktestResult with all performance metrics
        """
        account = self._broker.get_account()

        total_return, total_return_pct = calculate_total_return(
            self._config.starting_balance, account.cash
        )

        sharpe = calculate_sharpe_ratio(self._equity_curve)

        if self._equity_curve and self._cash_balance_curve:
            max_dd, max_dd_pct = calculate_max_drawdown(
                self._equity_curve, self._cash_balance_curve
            )
        else:
            max_dd, max_dd_pct = 0.0, 0.0

        all_orders = list(self._broker._orders.values())
        starting_dec = Decimal(str(self._config.starting_balance))
        unrealised_pnl = account.equity - starting_dec - Decimal(str(total_return))

        logger.info(
            f"Backtest complete: Return=${total_return:.2f} ({total_return_pct:.2f}%), "
            f"Sharpe={sharpe:.2f}, MaxDD=${max_dd:.2f} ({max_dd_pct:.2f}%)"
        )

        return SpotBacktestResult(
            config=self._config,
            realised_pnl=total_return,
            unrealised_pnl=float(unrealised_pnl),
            total_return_pct=total_return_pct,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd_pct,
            equity_curve=self._equity_curve,
            orders=all_orders,
            total_orders=len(all_orders),
        )
