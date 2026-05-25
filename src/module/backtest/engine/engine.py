import logging
from datetime import datetime

from module.broker import Order
from module.broker.enums import OrderSide, OrderStatus, OrderType
from module.markets.schema import OHLC
from module.strategy.strategy import BaseStrategy
from .schema import EquityCurvePoint, BacktestMetrics
from .oms_client import BacktestOMSClient
from .ohlc_feed_client import BacktestOHLCFeedClient

logger = logging.getLogger(__name__)


class BacktestEngine:
    """Engine for running backtests on strategy."""

    def __init__(
        self,
        strategy: BaseStrategy,
        starting_balance: float,
        start_date: datetime,
        end_date: datetime,
    ):
        self._strategy = strategy
        self._broker_client: BacktestOMSClient = self._strategy.oms_client  # type: ignore
        self._starting_balance = starting_balance
        self._start_date = start_date
        self._end_date = end_date

        self._equity_curve: list[EquityCurvePoint] = []
        self._balance_curve: list[EquityCurvePoint] = []

    def run(self) -> BacktestMetrics:
        """Run the backtest by streaming candles from database.

        Returns:
            BacktestMetrics object with results
        """
        logger.info(f"Starting backtest from {self._start_date} to {self._end_date}")
        logger.info(f"Starting balance: ${self._broker_client.get_balance():,.2f}")
        self._strategy.oms_client.ohlc_feed_client = self._strategy.ohlc_feed_client

        self._strategy.startup()
        self._process_candles()

        logger.info("Backtest completed")
        metrics = self._calculate_metrics()
        self._strategy.shutdown()
        return metrics

    def _process_candles(self):
        candle_count = 0
        last_log_count = 0
        log_interval = 100  # Log every 100 candles

        for candle in self._yield_candles(self._strategy.ohlc_feed_client):
            if candle_count == 0:
                self._equity_curve.append(
                    EquityCurvePoint(
                        timestamp=candle.timestamp,
                        equity=self._broker_client.get_equity(),
                        balance=self._broker_client.get_balance(),
                    )
                )

            candle_count += 1
            self._strategy.on_candle(candle)

            self._equity_curve.append(
                EquityCurvePoint(
                    timestamp=candle.timestamp,
                    equity=self._broker_client.get_equity(),
                    balance=self._broker_client.get_balance(),
                )
            )

            if candle_count - last_log_count >= log_interval:
                orders_placed = len(self._broker_client.get_orders())
                logger.info(
                    f"Progress: {candle_count} candles processed | "
                    f"Timestamp: {candle.timestamp} | "
                    f"Balance: ${self._broker_client.get_balance():,.2f} | "
                    f"Equity: ${self._broker_client.get_equity():,.2f} | "
                    f"Orders: {orders_placed}"
                )
                last_log_count = candle_count

        logger.info(
            f"Candle processing complete: {candle_count} total candles processed"
        )

    def _yield_candles(self, ohlc_feed_client: BacktestOHLCFeedClient):
        """Yield candles from feed client."""
        oms_client: BacktestOMSClient = self._strategy.oms_client  # type: ignore
        prev_candles: list[OHLC] = []

        for candle in ohlc_feed_client.candles():
            oms_client.execute_pending_orders(candle)
            prev_candles.append(candle)

            for subscription in ohlc_feed_client._subscriptions:
                if (
                    subscription["symbol"] == candle.symbol
                    and subscription["broker_type"] == candle.broker
                    and subscription["market_type"] == candle.market_type
                ):
                    for tf in subscription["timeframe"]:
                        tf_seconds = tf.get_seconds()

                        start_time = candle.timestamp // tf_seconds * tf_seconds
                        end_time = start_time + tf_seconds

                        if candle.timestamp + tf_seconds == end_time:
                            high: float = 0.0
                            low: float = 0.0
                            volume: float = 0.0

                            for i in range(len(prev_candles) - 1, -1, -1):
                                prev_candle = prev_candles[i]
                                if (
                                    prev_candle.symbol != candle.symbol
                                    or prev_candle.broker != candle.broker
                                    or prev_candle.market_type != candle.market_type
                                    or prev_candle.timeframe != candle.timeframe
                                ):
                                    continue

                                high = max(high, prev_candle.high)
                                low = min(low, prev_candle.low)
                                volume += prev_candle.volume

                                if prev_candle.timestamp == start_time:
                                    yield OHLC(
                                        open=prev_candle.open,
                                        high=high,
                                        low=low,
                                        close=candle.close,
                                        volume=volume,
                                        symbol=candle.symbol,
                                        broker=candle.broker,
                                        market_type=candle.market_type,
                                        timeframe=tf,
                                        timestamp=prev_candle.timestamp,
                                    )
                                    break

    def _calculate_metrics(self) -> BacktestMetrics:
        """Calculate backtest metrics.

        Returns:
            BacktestMetrics object
        """
        orders = self._broker_client.get_orders()

        end_balance = self._broker_client.get_balance()
        realised_pnl = end_balance - self._starting_balance
        end_equity = self._broker_client.get_equity()
        total_return_pct = (
            end_balance - self._starting_balance
        ) / self._starting_balance

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

        for order in self._broker_client.get_orders():
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
