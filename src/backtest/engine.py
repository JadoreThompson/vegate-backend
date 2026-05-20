import logging
from datetime import timedelta

import numpy as np

from backtest.config import BacktestConfig
from enums import OrderSide, OrderStatus, OrderType
from models import OHLC, BacktestMetrics, EquityCurvePoint, Order
from service.ohlc.feed.backtest.client import BacktestOHLCFeedClient
from service.oms.broker_client.backtest import BacktestBrokerClient
from strategy.strategy import Strategy

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Engine for running backtests on strategy."""

    def __init__(self, strategy: Strategy, config: BacktestConfig):
        """Initialize the backtesting engine.

        Args:
            strategy: Strategy instance to run
            broker: BacktestBroker instance for order execution
            config: BacktestConfig object
        """
        self._strategy = strategy
        self._broker: BacktestBrokerClient = self._strategy.oms_client  # type: ignore
        self._config = config
        self._equity_curve: list[EquityCurvePoint] = []
        self._balance_curve: list[EquityCurvePoint] = []

    def run(self) -> BacktestMetrics:
        """Run the backtest by streaming candles from database.

        Returns:
            BacktestMetrics object with results
        """
        logger.info(
            f"Starting backtest: {self._config.symbol} ({self._config.timeframe}) "
            f"from {self._config.start_date} to {self._config.end_date}"
        )
        logger.info(f"Starting balance: ${self._broker.get_balance():,.2f}")

        self._strategy.startup()
        self._process_candles()

        logger.info("Backtest completed")
        metrics = self._calculate_metrics()
        self._strategy.shutdown()
        return metrics

    def _process_candles(self) -> None:
        """Stream and process candles from database."""
        candle_count = 0
        last_log_count = 0
        log_interval = 100  # Log every 100 candles

        ohlc_feed_client: BacktestOHLCFeedClient = self._strategy.ohlc_feed_client
        self._broker.ohlc_feed_client = ohlc_feed_client
        tf_seconds = ohlc_feed_client.timeframe.get_seconds()
        start = ohlc_feed_client.start
        open = None
        high = None
        low = None
        close = None
        volume = 0

        for candle in ohlc_feed_client.candles():
            if candle_count == 0:
                start = candle.timestamp
                self._equity_curve.append(
                    EquityCurvePoint(
                        timestamp=candle.timestamp,
                        equity=self._broker.get_equity(),
                        balance=self._broker.get_balance(),
                    )
                )

            candle_count += 1

            self._broker.execute_pending_orders(candle)

            # self._strategy.on_candle(candle)

            if open is None:
                open = candle.open

            high = candle.high if high is None else max(high, candle.high)
            low = candle.low if low is None else min(low, candle.low)
            close = candle.close
            volume += candle.volume

            if candle.timestamp + tf_seconds == start + tf_seconds:
                candle = OHLC(
                    open=open,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    symbol=candle.symbol,
                    broker=candle.broker,
                    market_type=candle.market_type,
                    timeframe=candle.timeframe,
                    timestamp=start,
                )

                self._strategy.on_candle(candle)

                open = None
                high = None
                low = None
                close = None
                volume = 0
                start = start + tf_seconds

            self._equity_curve.append(
                EquityCurvePoint(
                    timestamp=candle.timestamp,
                    equity=self._broker.get_equity(),
                    balance=self._broker.get_balance(),
                )
            )

            if candle_count - last_log_count >= log_interval:
                orders_placed = len(self._broker.get_orders())
                logger.info(
                    f"Progress: {candle_count} candles processed | "
                    f"Timestamp: {candle.timestamp} | "
                    f"Balance: ${self._broker.get_balance():,.2f} | "
                    f"Equity: ${self._broker.get_equity():,.2f} | "
                    f"Orders: {orders_placed}"
                )
                last_log_count = candle_count

        logger.info(
            f"Candle processing complete: {candle_count} total candles processed"
        )

    def _calculate_metrics(self) -> BacktestMetrics:
        """Calculate backtest metrics.

        Returns:
            BacktestMetrics object
        """
        orders = self._broker.get_orders()

        end_balance = self._broker.get_balance()
        realised_pnl = end_balance - self._config.starting_balance
        end_equity = self._broker.get_equity()
        total_return_pct = (
            end_balance - self._config.starting_balance
        ) / self._config.starting_balance

        return BacktestMetrics(
            realised_pnl=realised_pnl,
            unrealised_pnl=end_equity - end_balance,
            total_return_pct=total_return_pct * 100,
            equity_curve=self._equity_curve,
            orders=orders,
            total_orders=len(orders),
            profit_factor=self._calculate_profit_factor(orders),
        )

    def _calculate_pnl(self) -> float:
        """Calculate total profit and loss."""

        total_notional = 0.0

        for order in self._broker.get_orders():
            if order.status not in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
                continue

            value = 0.0

            if order.notional is not None:
                value = order.notional
            else:
                if order.order_type == OrderType.MARKET:
                    price = order.avg_fill_price
                elif order.order_type == OrderType.LIMIT:
                    price = order.limit_price
                else:
                    price = order.stop_price

                value = price * order.filled_quantity

            if order.side == OrderSide.BUY:
                total_notional -= value
            else:
                total_notional += value

        return total_notional

    def _calculate_sharpe_ratio(self) -> float:
        """Calculate Sharpe ratio based on backtest duration.

        Returns:
            Sharpe ratio value
        """
        if len(self._equity_curve) < 2:
            return 0.0

        # Calculate backtest duration
        duration = self._config.end_date - self._config.start_date
        duration_days = duration.days

        # Determine the resampling period based on duration
        if duration_days < 1:
            # Less than 1 day - calculate directly from all points
            return self._calculate_sharpe_from_equity_curve(
                self._equity_curve,
                periods_per_year=252 * 24 * 60,  # Assuming minute data
            )
        elif duration_days < 7:
            # 1 day to 1 week - use daily returns
            return self._calculate_sharpe_with_resampling(
                self._equity_curve, timedelta(days=1), periods_per_year=252
            )
        elif duration_days < 30:
            # 1 week to 1 month - use weekly returns
            return self._calculate_sharpe_with_resampling(
                self._equity_curve, timedelta(days=7), periods_per_year=52
            )
        elif duration_days < 365:
            # 1 month to 1 year - use monthly returns
            return self._calculate_sharpe_with_resampling(
                self._equity_curve, timedelta(days=30), periods_per_year=12
            )
        else:
            # 1 year or more - use yearly returns
            return self._calculate_sharpe_with_resampling(
                self._equity_curve, timedelta(days=365), periods_per_year=1
            )

    def _calculate_sharpe_from_equity_curve(
        self, equity_curve: list[EquityCurvePoint], periods_per_year: float
    ) -> float:
        """Calculate Sharpe ratio from equity curve points.

        Args:
            equity_curve: List of equity curve points
            periods_per_year: Number of periods per year for annualization

        Returns:
            Sharpe ratio value
        """
        if len(equity_curve) < 2:
            return 0.0

        # Extract equity values
        equity_values = np.array([point.value for point in equity_curve])

        # Calculate returns
        returns = np.diff(equity_values) / equity_values[:-1]

        if len(returns) == 0:
            return 0.0

        # Need at least 2 returns to calculate std with ddof=1
        if len(returns) < 2:
            return 0.0

        # Calculate mean and std of returns
        mean_return = np.mean(returns)
        std_return = np.std(returns, ddof=1)

        if std_return == 0 or np.isnan(std_return):
            return 0.0

        # Calculate Sharpe ratio (assuming risk-free rate = 0)
        sharpe_ratio = (mean_return / std_return) * np.sqrt(periods_per_year)

        # Handle NaN or inf results
        if np.isnan(sharpe_ratio) or np.isinf(sharpe_ratio):
            return 0.0

        return float(sharpe_ratio)

    def _calculate_sharpe_with_resampling(
        self,
        equity_curve: list[EquityCurvePoint],
        period: timedelta,
        periods_per_year: float,
    ) -> float:
        """Calculate Sharpe ratio with resampling to specific period.

        Args:
            equity_curve: List of equity curve points
            period: Time period for resampling (e.g., timedelta(days=1) for daily)
            periods_per_year: Number of periods per year for annualization

        Returns:
            Sharpe ratio value
        """
        if len(equity_curve) < 2:
            return 0.0

        # Resample equity curve to the specified period
        # Strategy: Take the last point in each period
        resampled_points = []
        period_seconds = int(period.total_seconds())

        first_timestamp = equity_curve[0].timestamp
        current_period_start = int(first_timestamp.timestamp())

        # Always include the first point
        resampled_points.append(equity_curve[0])

        last_added_point = equity_curve[0]

        for point in equity_curve[1:]:  # Skip first point since we already added it
            point_timestamp = int(point.timestamp.timestamp())

            # Check if we've moved to a new period
            if point_timestamp >= current_period_start + period_seconds:
                # Add the last point from the previous period (if not already added)
                if last_added_point != resampled_points[-1]:
                    resampled_points.append(last_added_point)

                # Move to next period
                # Calculate how many periods we've crossed
                periods_crossed = (
                    point_timestamp - current_period_start
                ) // period_seconds
                current_period_start += periods_crossed * period_seconds

            last_added_point = point

        # Add the last point if not already included
        if last_added_point != resampled_points[-1]:
            resampled_points.append(last_added_point)

        return self._calculate_sharpe_from_equity_curve(
            resampled_points, periods_per_year
        )

    def _calculate_profit_factor(self, orders: list[Order]) -> float:
        """Calculate profit factor from list of orders.

        Args:
            orders: List of Order objects

        Returns:
            Profit factor (gross profit / gross loss), or 0.0 if no losing trades
        """
        gross_profit = 0.0
        gross_loss = 0.0

        positions: dict[str, dict] = {}

        for order in orders:
            if order.status not in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
                continue

            symbol = order.symbol
            if symbol not in positions:
                positions[symbol] = {"qty": 0.0, "avg_price": 0.0}

            pos = positions[symbol]

            # Resolve the executed price
            if order.avg_fill_price is not None:
                price = order.avg_fill_price
            elif order.order_type == OrderType.LIMIT:
                price = order.limit_price
            elif order.order_type == OrderType.MARKET:
                price = order.avg_fill_price  # best available
            else:
                price = order.stop_price

            if price is None:
                logger.warning(f"Order {order.id} has no resolvable price, skipping")
                continue

            qty = order.filled_quantity

            if order.side == OrderSide.BUY:
                total_cost = pos["qty"] * pos["avg_price"] + qty * price
                pos["qty"] += qty
                pos["avg_price"] = total_cost / pos["qty"] if pos["qty"] > 0 else 0.0

            elif order.side == OrderSide.SELL:
                pnl = qty * (price - pos["avg_price"])
                pos["qty"] -= qty

                if pos["qty"] <= 0:
                    pos["qty"] = 0.0
                    pos["avg_price"] = 0.0

                if pnl >= 0:
                    gross_profit += pnl
                else:
                    gross_loss += abs(pnl)

        if gross_loss == 0.0:
            return float("inf") if gross_profit > 0 else 0.0

        return round(gross_profit / gross_loss, 2)
