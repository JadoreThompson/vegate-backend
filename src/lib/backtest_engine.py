import logging
from datetime import UTC, datetime

from sqlalchemy import select

from infra.db.models.ohlcs import OHLCs
from infra.db.utils import get_db_sess_sync
from lib.brokers import BacktestBroker
from models import (
    OHLC,
    BacktestMetrics,
    Order,
    EquityCurvePoint,
    BacktestConfig,
)
from enums import OrderStatus, OrderType
from lib.strategy import BaseStrategy


logger = logging.getLogger(__name__)


class BacktestEngine:
    """Engine for running backtests on strategies."""

    def __init__(
        self, strategy: BaseStrategy, broker: BacktestBroker, config: BacktestConfig
    ):
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
        start_date = self.config.start_date
        end_date = self.config.end_date

        with get_db_sess_sync() as db_sess:
            results = db_sess.scalars(
                select(OHLCs)
                .where(
                    OHLCs.source == self.config.broker,
                    OHLCs.symbol == self.config.symbol,
                    OHLCs.timeframe == self.config.timeframe,
                    OHLCs.timestamp
                    >= int(
                        datetime(
                            year=start_date.year,
                            month=start_date.month,
                            day=start_date.day,
                            tzinfo=UTC,
                        ).timestamp()
                    ),
                    OHLCs.timestamp
                    <= int(
                        datetime(
                            year=end_date.year,
                            month=end_date.month,
                            day=end_date.day,
                            tzinfo=UTC,
                        ).timestamp()
                    ),
                )
                .order_by(OHLCs.timestamp.asc())
            )

            for res in results.yield_per(1000):
                candle = OHLC(
                    open=res.open,
                    high=res.high,
                    low=res.low,
                    close=res.close,
                    volume=0.0,
                    timestamp=res.timestamp,
                    timeframe=res.timeframe,
                    symbol=res.symbol,
                )
                self._record_equity_point(candle)
                self.strategy.on_candle(candle)

    def _record_equity_point(self, candle: OHLC) -> None:
        """Record equity curve point for current candle."""
        self.equity_curve.append(
            EquityCurvePoint(timestamp=candle.timestamp, equity=self.broker.balance)
        )

    def _calculate_metrics(self) -> BacktestMetrics:
        """Calculate backtest metrics.

        Returns:
            BacktestMetrics object
        """
        orders = self.broker.get_orders()

        realised_pnl = self._calculate_pnl()
        end_balance = self.config.starting_balance + realised_pnl
        total_return_pct = (
            end_balance - self.config.starting_balance
        ) / self.config.starting_balance

        # TODO: finish metrics
        return BacktestMetrics(
            config=self.config,
            realised_pnl=realised_pnl,
            unrealised_pnl=0.0,
            total_return_pct=total_return_pct,
            equity_curve=self.equity_curve,
            sharpe_ratio=1.0,
            max_drawdown=0,
            orders=orders,
            total_orders=len(orders),
        )

    def _calculate_pnl(self) -> float:
        """Calculate total profit and loss."""
        total_buy_notional = 0.0
        for order in self.broker.buy_orders:
            if order.notional is not None:
                total_buy_notional += order.notional
            else:
                if order.order_type == OrderType.MARKET:
                    price = order.price
                elif order.order_type == OrderType.LIMIT:
                    price = order.limit_price
                else:
                    price = order.stop_price

                total_buy_notional += price * order.quantity

        total_buy_notional = self._get_notional(self.broker.buy_orders)
        total_sell_notional = self._get_notional(self.broker.sell_orders)
        return total_sell_notional - total_buy_notional

    def _get_notional(self, orders: list[Order]):
        total_notional = 0.0

        for order in orders:
            if order.status not in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
                continue

            if order.notional is not None:
                total_notional += order.notional
            else:
                if order.order_type == OrderType.MARKET:
                    price = order.price
                elif order.order_type == OrderType.LIMIT:
                    price = order.limit_price
                else:
                    price = order.stop_price

                total_notional += price * order.executed_quantity

        return total_notional
