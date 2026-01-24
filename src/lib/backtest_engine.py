import logging

from enums import OrderStatus, OrderType
from lib.brokers import BacktestBroker
from lib.strategy import BaseStrategy
from models import (
    OHLC,
    BacktestMetrics,
    Order,
    EquityCurvePoint,
    BacktestConfig,
)


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
        logger.info(
            f"Starting backtest: {self.config.symbol} ({self.config.timeframe}) "
            f"from {self.config.start_date} to {self.config.end_date}"
        )
        logger.info(f"Starting balance: ${self.broker.get_balance():,.2f}")

        self.strategy.startup()
        self._process_candles()
        self.strategy.shutdown()

        logger.info("Backtest completed")
        return self._calculate_metrics()

    def _process_candles(self) -> None:
        """Stream and process candles from database."""
        candle_count = 0
        last_log_count = 0
        log_interval = 100  # Log every 100 candles

        for candle in self.broker.stream_candles(
            self.config.symbol,
            self.config.timeframe,
            self.config.broker,
            self.config.start_date,
            self.config.end_date,
        ):
            candle_count += 1

            # Update equity calculation
            self.broker._calculate_equity()

            self.equity_curve.append(
                EquityCurvePoint(
                    timestamp=candle.timestamp, equity=self.broker.get_equity()
                )
            )
            self.strategy.on_candle(candle)

            # Log progress at intervals
            if candle_count - last_log_count >= log_interval:
                orders_placed = len(self.broker.get_orders())
                logger.info(
                    f"Progress: {candle_count} candles processed | "
                    f"Timestamp: {candle.timestamp} | "
                    f"Balance: ${self.broker.get_balance():,.2f} | "
                    f"Equity: ${self.broker.get_equity():,.2f} | "
                    f"Orders: {orders_placed}"
                )
                last_log_count = candle_count

        logger.info(
            f"Candle processing complete: {candle_count} total candles processed"
        )

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

        # Log final metrics
        logger.info("=" * 60)
        logger.info("BACKTEST RESULTS")
        logger.info("=" * 60)
        logger.info(f"Symbol: {self.config.symbol}")
        logger.info(f"Timeframe: {self.config.timeframe}")
        logger.info(f"Period: {self.config.start_date} to {self.config.end_date}")
        logger.info(f"Starting Balance: ${self.config.starting_balance:,.2f}")
        logger.info(f"Ending Balance: ${end_balance:,.2f}")
        logger.info(f"Realised P&L: ${realised_pnl:,.2f}")
        logger.info(f"Total Return: {total_return_pct * 100:.2f}%")
        logger.info(f"Total Orders: {len(orders)}")
        logger.info(f"Final Equity: ${self.broker.get_equity():,.2f}")
        logger.info("=" * 60)

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
