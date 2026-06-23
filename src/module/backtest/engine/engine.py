import logging
from datetime import datetime

from module.broker.client.exception import BrokerClientException
from vegate.markets.schema import OHLC
from vegate.oms.enums import OrderSide, OrderStatus, OrderType
from vegate.oms.schema import Order
from vegate.strategy.base import BaseStrategy
from .ohlc_feed_client import BacktestOHLCFeedClient
from .schema import EquityCurvePoint, BacktestMetrics


class BacktestEngine:
    """Engine for running backtests on a strategy using the configured
    OMS client (spot or futures)."""

    def __init__(
        self,
        strategy: BaseStrategy,
        starting_balance: float,
        start_date: datetime,
        end_date: datetime,
    ):
        self._strategy = strategy
        self._broker_client = strategy.oms_client
        self._starting_balance = starting_balance
        self._start_date = start_date
        self._end_date = end_date

        self._equity_curve: list[EquityCurvePoint] = []
        self._logger = logging.getLogger(__name__)

    def run(self) -> BacktestMetrics:
        self._logger.info(
            f"Starting backtest from {self._start_date} to {self._end_date}"
        )
        self._logger.info(
            f"Starting balance: ${self._broker_client.get_balance():,.2f}"
        )
        self._strategy.oms_client.ohlc_feed_client = self._strategy.ohlc_feed_client

        self._strategy.startup()
        self._process_candles()

        self._logger.info("Backtest completed")
        metrics = self._calculate_metrics()
        self._strategy.shutdown()
        return metrics

    def _process_candles(self):
        candle_count = 0
        last_log_count = 0
        log_interval = 100

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
            try:
                self._strategy.on_candle(candle)
            except BrokerClientException as e:
                self._logger.error(e)

            self._equity_curve.append(
                EquityCurvePoint(
                    timestamp=candle.timestamp,
                    equity=self._broker_client.get_equity(),
                    balance=self._broker_client.get_balance(),
                )
            )

            if candle_count - last_log_count >= log_interval:
                orders = len(self._broker_client.get_orders())
                self._logger.info(
                    f"Progress: {candle_count} candles | "
                    f"Balance: ${self._broker_client.get_balance():,.2f} | "
                    f"Equity: ${self._broker_client.get_equity():,.2f} | "
                    f"Orders: {orders}"
                )
                last_log_count = candle_count

    def _yield_candles(self, ohlc_feed_client: BacktestOHLCFeedClient):
        oms_client = self._strategy.oms_client
        prev_candles: list[OHLC] = []

        for candle in ohlc_feed_client.candles():
            oms_client.execute_pending_orders(candle)
            prev_candles.append(candle)

            for sub in ohlc_feed_client._subscriptions:
                if (
                    sub["symbol"] == candle.symbol
                    and sub["broker_type"] == candle.broker
                    and sub["market_type"] == candle.market_type
                ):
                    for tf in sub["timeframe"]:
                        tf_seconds = tf.get_seconds()
                        start_time = candle.timestamp // tf_seconds * tf_seconds
                        end_time = start_time + tf_seconds

                        if candle.timestamp + tf_seconds == end_time:
                            high = low = 0.0
                            volume = 0.0
                            for i in range(len(prev_candles) - 1, -1, -1):
                                pc = prev_candles[i]
                                if (
                                    pc.symbol != candle.symbol
                                    or pc.broker != candle.broker
                                    or pc.market_type != candle.market_type
                                    or pc.timeframe != candle.timeframe
                                ):
                                    continue
                                high = max(high, pc.high)
                                low = min(low, pc.low)
                                volume += pc.volume
                                if pc.timestamp == start_time:
                                    yield OHLC(
                                        open=pc.open, high=high, low=low,
                                        close=candle.close, volume=volume,
                                        symbol=candle.symbol, broker=candle.broker,
                                        market_type=candle.market_type,
                                        timeframe=tf, timestamp=pc.timestamp,
                                    )
                                    break

    def _calculate_metrics(self) -> BacktestMetrics:
        orders = self._broker_client.get_orders()
        end_balance = self._broker_client.get_balance()
        realised_pnl = end_balance - self._starting_balance
        end_equity = self._broker_client.get_equity()
        total_return_pct = (
            realised_pnl / self._starting_balance if self._starting_balance else 0.0
        )

        return BacktestMetrics(
            realised_pnl=realised_pnl,
            unrealised_pnl=end_equity - end_balance,
            total_return_pct=total_return_pct * 100,
            equity_curve=self._equity_curve,
            orders=orders,
            total_orders=len(orders),
            profit_factor=self._calculate_profit_factor(orders),
        )

    def _calculate_profit_factor(self, orders: list[Order]) -> float:
        gross_profit = 0.0
        gross_loss = 0.0
        positions: dict[str, dict] = {}

        for order in orders:
            if order.status not in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
                continue
            price = order.avg_fill_price or order.limit_price or order.stop_price
            if price is None:
                continue

            symbol = order.symbol
            if symbol not in positions:
                positions[symbol] = {"qty": 0.0, "avg_price": 0.0}

            pos = positions[symbol]
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
