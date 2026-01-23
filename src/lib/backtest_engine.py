import logging
from dataclasses import dataclass
from datetime import datetime

from lib.brokers import BacktestBroker
from models import (
    OHLC,
    BacktestMetrics,
    Order,
    EquityCurvePoint,
)
from enums import OrderStatus, BrokerType
from lib.strategy import BaseStrategy


logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for backtesting."""

    timeframe: str
    starting_balance: float
    symbol: str
    start_date: datetime
    end_date: datetime
    broker: BrokerType


class BacktestEngine:
    """Engine for running backtests on strategies."""

    def __init__(self, strategy: BaseStrategy, broker: BacktestBroker, config: BacktestConfig):
        """Initialize the backtesting engine.

        Args:
            strategy: Strategy instance to run
            broker: BacktestBroker instance for order execution
            config: BacktestConfig object
        """
        self.strategy = strategy
        self.broker = broker
        self.config = config
        self.equity_curve: list[EquityCurvePoint] = []

    def run(self) -> BacktestMetrics:
        """Run the backtest by streaming candles from database.

        Returns:
            BacktestMetrics object with results
        """
        self.strategy.startup()
        self._process_candles()
        self.strategy.shutdown()

        return self._calculate_metrics()

    def _process_candles(self) -> None:
        """Stream and process candles from database."""
        for candle in self.broker.stream_candles(
            self.config.symbol,
            self.config.timeframe,
            self.config.broker
        ):
            if self.config.start_date <= candle.timestamp < self.config.end_date:
                self._record_equity_point(candle)
                self.strategy.on_candle(candle)

    def _record_equity_point(self, candle: OHLC) -> None:
        """Record equity curve point for current candle."""
        self.equity_curve.append(EquityCurvePoint(
            timestamp=candle.timestamp,
            equity=self.broker.balance
        ))

    def _calculate_metrics(self) -> BacktestMetrics:
        """Calculate backtest metrics.

        Returns:
            BacktestMetrics object
        """
        orders = self.broker.get_orders()
        filled_orders = [o for o in orders if o.status == OrderStatus.FILLED]

        total_pnl = self._calculate_pnl()
        ending_balance = self.config.starting_balance + total_pnl
        trade_stats = self._calculate_trade_stats(filled_orders)
        total_return_percent = self._calculate_return_percent(total_pnl)

        return BacktestMetrics(
            total_pnl=total_pnl,
            highest_balance=self.config.starting_balance
            + max((o.notional for o in filled_orders), default=0),
            lowest_balance=self.config.starting_balance
            + min((o.notional for o in filled_orders), default=0),
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbol=self.config.symbol,
            orders=orders,
            starting_balance=self.config.starting_balance,
            ending_balance=ending_balance,
            total_return_percent=total_return_percent,
            num_trades=trade_stats['num_trades'],
            winning_trades=trade_stats['winning_trades'],
            losing_trades=trade_stats['losing_trades'],
            win_rate=trade_stats['win_rate'],
            avg_win=trade_stats['avg_win'],
            avg_loss=trade_stats['avg_loss'],
            profit_factor=trade_stats['profit_factor'],
            equity_curve=self.equity_curve,
        )

    def _calculate_pnl(self) -> float:
        """Calculate total profit and loss."""
        total_buy_notional = sum(
            o.notional for o in self.broker.buy_orders if o.status == OrderStatus.FILLED
        )
        total_sell_notional = sum(
            abs(o.notional)
            for o in self.broker.sell_orders
            if o.status == OrderStatus.FILLED
        )
        return total_sell_notional - total_buy_notional

    def _calculate_trade_stats(self, filled_orders: list[Order]) -> dict:
        """Calculate trade statistics.

        Args:
            filled_orders: List of filled orders

        Returns:
            Dictionary with trade statistics
        """
        num_trades = len(filled_orders)
        winning_trades = sum(1 for o in filled_orders if o.notional > 0)
        losing_trades = sum(1 for o in filled_orders if o.notional < 0)
        win_rate = (winning_trades / num_trades * 100) if num_trades > 0 else 0

        winning_notionals = [o.notional for o in filled_orders if o.notional > 0]
        losing_notionals = [abs(o.notional) for o in filled_orders if o.notional < 0]

        avg_win = (
            sum(winning_notionals) / len(winning_notionals) if winning_notionals else 0
        )
        avg_loss = (
            sum(losing_notionals) / len(losing_notionals) if losing_notionals else 0
        )

        profit_factor = (
            (sum(winning_notionals) / sum(losing_notionals))
            if losing_notionals and sum(losing_notionals) > 0
            else 0
        )

        return {
            'num_trades': num_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
        }

    def _calculate_return_percent(self, total_pnl: float) -> float:
        """Calculate total return percentage.

        Args:
            total_pnl: Total profit and loss

        Returns:
            Total return as percentage
        """
        return (
            (total_pnl / self.config.starting_balance * 100)
            if self.config.starting_balance > 0
            else 0
        )
