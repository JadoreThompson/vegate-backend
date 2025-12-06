import logging
from datetime import datetime

from core.models import CustomBaseModel
from engine.brokers import BacktestBroker
from engine.enums import Timeframe, OrderStatus, OrderSide
from engine.models import OrderResponse
from engine.strategy import BaseStrategy
from engine.strategy import StrategyContext, StrategyManager
from .metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_total_return,
)
from .types import EquityCurveT


logger = logging.getLogger(__name__)


class BacktestConfig(CustomBaseModel):
    start_date: datetime
    end_date: datetime
    symbol: str
    starting_balance: float = 100_000.0
    timeframe: Timeframe


class BacktestResult(CustomBaseModel):
    config: BacktestConfig
    realised_pnl: float
    unrealised_pnl: float
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    equity_curve: EquityCurveT


class SpotBacktestResult(BacktestResult):
    orders: list[OrderResponse]
    total_orders: int


class BacktestEngine:
    def __init__(self, config: BacktestConfig, strategy: BaseStrategy):
        self._config = config
        self._strategy = strategy
        self._broker = BacktestBroker(starting_balance=self._config.starting_balance)
        self._strategy_runner = StrategyManager(self._strategy, self._broker)
        self._strategy_context = StrategyContext[BacktestBroker](self._broker)

        self._equity_curve: EquityCurveT = []
        self._cash_balance_curve: EquityCurveT = []

    def run(self) -> SpotBacktestResult:
        for ohlcv in self._broker.yield_historic_ohlcv(
            self._config.symbol, self._config.start_date, self._config.end_date
        ):
            account = self._broker.get_account()
            self._equity_curve.append((ohlcv.timestamp, account.equity))
            self._cash_balance_curve.append((ohlcv.timestamp, account.cash))
            self._strategy_runner.process(self._strategy_context)

        self._broker.cancel_all_orders()
        return self._calculate_results()

    def _calculate_results(self) -> SpotBacktestResult:
        """
        Calculate comprehensive performance metrics.

        Returns:
            SpotBacktestResult with all performance metrics
        """
        account = self._broker.get_account()

        # Calculate returns
        total_return, total_return_pct = calculate_total_return(
            self._config.starting_balance, account.equity
        )

        # Calculate risk metrics
        sharpe = calculate_sharpe_ratio(self._equity_curve)
        max_dd, max_dd_pct = calculate_max_drawdown(
            self._equity_curve, self._cash_balance_curve
        )

        # Get all orders from broker
        all_orders = list(self._broker._orders.values())

        # Calculate realized and unrealized PnL
        realised_pnl = 0.0
        unrealised_pnl = 0.0

        for order in all_orders:
            if order.status == OrderStatus.FILLED:
                if order.side == OrderSide.SELL and order.avg_fill_price:
                    realised_pnl += order.quantity * order.avg_fill_price
                elif order.side == OrderSide.BUY and order.avg_fill_price:
                    realised_pnl -= order.quantity * order.avg_fill_price

        unrealised_pnl = account.equity - self._config.starting_balance - realised_pnl

        logger.info(
            f"Backtest complete: Return=${total_return:.2f} ({total_return_pct:.2f}%), "
            f"Sharpe={sharpe:.2f}, MaxDD=${max_dd:.2f} ({max_dd_pct:.2f}%)"
        )

        return SpotBacktestResult(
            config=self._config,
            realised_pnl=realised_pnl,
            unrealised_pnl=unrealised_pnl,
            total_return=total_return_pct,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd_pct,
            equity_curve=self._equity_curve,
            orders=all_orders,
            total_orders=len(all_orders),
        )
