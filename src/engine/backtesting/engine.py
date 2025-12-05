import logging
import time as time_module
from typing import Callable, List, Dict, Tuple
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass

from .data_loader import OHLCDataLoader, OHLCBar, Timeframe, TradeRecord
from .simulated_broker import SimulatedBroker
from .metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_win_rate,
    calculate_total_return,
)
from ..models import OrderSide

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """
    Configuration for backtest execution.

    Attributes:
        start_date: Start date for backtest (inclusive)
        end_date: End date for backtest (inclusive)
        symbols: List of symbols to trade
        initial_capital: Starting capital (default: 100000.0)
        commission_per_share: Fixed commission per share (default: 0.0)
        commission_percent: Commission as percentage of trade value (default: 0.0)
        slippage_percent: Slippage as percentage of price (default: 0.1%)
        timeframe: Bar timeframe (default: 1 minute)
        allow_fractional_shares: Whether to allow fractional shares (default: False)
        enable_slippage: Whether to apply slippage (default: True)
        enable_commissions: Whether to apply commissions (default: True)
    """

    start_date: datetime
    end_date: datetime
    symbols: List[str]
    initial_capital: float = 100000.0
    commission_per_share: float = 0.0
    commission_percent: float = 0.0
    slippage_percent: float = 0.1
    timeframe: Timeframe = Timeframe.M1
    allow_fractional_shares: bool = False
    enable_slippage: bool = True
    enable_commissions: bool = True


@dataclass
class BacktestResult:
    """
    Results from backtest execution.

    Attributes:
        config: Backtest configuration used
        initial_capital: Starting capital
        final_capital: Ending capital
        total_return: Total return in dollars
        total_return_percent: Total return as percentage
        sharpe_ratio: Risk-adjusted return metric
        max_drawdown: Maximum drawdown in dollars
        max_drawdown_percent: Maximum drawdown as percentage
        total_trades: Total number of trades executed
        winning_trades: Number of profitable trades
        losing_trades: Number of losing trades
        win_rate: Percentage of winning trades
        equity_curve: List of (timestamp, equity) tuples
        trades: List of executed trades
        execution_time_seconds: Total execution time
    """

    config: BacktestConfig
    initial_capital: float
    final_capital: float
    total_return: float
    total_return_percent: float
    sharpe_ratio: float
    max_drawdown: float
    max_drawdown_percent: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    equity_curve: List[Tuple[datetime, float]]
    trades: List[TradeRecord]
    execution_time_seconds: float


class BacktestEngine:
    """
    Main backtesting engine that orchestrates strategy simulation.

    This engine loads historical OHLC data, simulates order execution through
    a simulated broker, tracks portfolio state, and calculates comprehensive
    performance metrics.

    The engine iterates chronologically through historical data, updating the
    broker's state and executing the strategy function at each timestamp.

    Example:
        config = BacktestConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),
            symbols=["AAPL", "TSLA"],
            initial_capital=100000.0
        )

        def my_strategy(context):
            # Strategy logic here
            price = context.close("AAPL")
            if price > 150:
                context.buy("AAPL", quantity=10)

        engine = BacktestEngine(config, data_loader, my_strategy)
        result = engine.run()
    """

    def __init__(
        self,
        config: BacktestConfig,
        data_loader: OHLCDataLoader,
        strategy_func: Callable,
    ):
        """
        Initialize backtest engine.

        Args:
            config: Backtest configuration
            data_loader: Data loader for historical OHLC data
            strategy_func: Callable that implements strategy logic
        """
        self.config = config
        self.data_loader = data_loader
        self.strategy_func = strategy_func

        # Initialize simulated broker
        self.broker = SimulatedBroker(
            initial_capital=config.initial_capital,
            commission_per_share=(
                config.commission_per_share if config.enable_commissions else 0.0
            ),
            commission_percent=(
                config.commission_percent if config.enable_commissions else 0.0
            ),
            slippage_percent=config.slippage_percent if config.enable_slippage else 0.0,
        )

        # Tracking
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.trades: List[TradeRecord] = []
        self.current_bars: Dict[str, OHLCBar] = {}
        self._trade_counter = 0

        logger.info(
            f"BacktestEngine initialized: {len(config.symbols)} symbols, "
            f"{config.start_date.date()} to {config.end_date.date()}"
        )

    def run(self) -> BacktestResult:
        """
        Execute the complete backtest.

        This method:
        1. Connects to the simulated broker
        2. Loads historical data in batches
        3. Iterates through each timestamp chronologically
        4. Updates broker state and executes strategy
        5. Tracks equity curve and trades
        6. Calculates performance metrics

        Returns:
            BacktestResult with comprehensive statistics

        Raises:
            Exception: If backtest execution fails
        """
        start_time = time_module.time()

        logger.info("Starting backtest execution...")

        try:
            self.broker.connect()

            # Track progress
            total_bars_processed = 0

            # Load and process data in batches
            for batch in self.data_loader.load_data(
                symbols=self.config.symbols,
                start_date=self.config.start_date,
                end_date=self.config.end_date,
                timeframe=self.config.timeframe,
            ):
                # Group bars by timestamp for event-driven processing
                bars_by_time = defaultdict(list)
                for bar in batch:
                    bars_by_time[bar.timestamp].append(bar)

                # Process each timestamp
                for timestamp in sorted(bars_by_time.keys()):
                    bars = bars_by_time[timestamp]
                    self._process_bars(timestamp, bars)
                    total_bars_processed += len(bars)

                # Log progress periodically
                if total_bars_processed % 10000 == 0:
                    logger.debug(f"Processed {total_bars_processed} bars...")

            self.broker.disconnect()

            # Calculate final metrics
            execution_time = time_module.time() - start_time
            result = self._calculate_results(execution_time)

            logger.info(
                f"Backtest complete: {total_bars_processed} bars processed in "
                f"{execution_time:.2f}s, final capital: ${result.final_capital:,.2f}"
            )

            return result

        except Exception as e:
            logger.error(f"Backtest execution failed: {e}", exc_info=True)
            raise

    def _process_bars(self, timestamp: datetime, bars: List[OHLCBar]) -> None:
        """
        Process all bars at a single timestamp.

        This method:
        1. Updates broker state with current time and prices
        2. Updates current bars cache
        3. Creates a context object for the strategy
        4. Executes the strategy function
        5. Records current equity

        Args:
            timestamp: Current timestamp
            bars: List of bars for all symbols at this timestamp
        """
        # Update broker state
        self.broker.set_current_time(timestamp)

        for bar in bars:
            self.broker.set_current_price(bar.symbol, bar.close)
            self.current_bars[bar.symbol] = bar

        # Create context for strategy (simplified version without full context object)
        # In production, this would use the StrategyContext from Module 3
        context = BacktestContext(
            timestamp=timestamp,
            bars=self.current_bars,
            broker=self.broker,
            data_loader=self.data_loader,
        )

        # Execute strategy
        try:
            self.strategy_func(context)
        except Exception as e:
            logger.error(f"Strategy execution error at {timestamp}: {e}", exc_info=True)
            # Continue execution despite strategy errors

        # Record equity point
        account = self.broker.get_account()
        self.equity_curve.append((timestamp, account.portfolio_value))

        # Track trades from broker orders
        self._update_trades()

    def _update_trades(self) -> None:
        """
        Update trade records from broker orders.

        This method tracks new filled orders and creates trade records
        for performance analysis.
        """
        # Get all orders from broker
        for order_id, order in self.broker.orders.items():
            # Check if we've already recorded this trade
            if any(t.trade_id == order_id for t in self.trades):
                continue

            # Only record filled orders
            if order.status.value == "filled":
                self._trade_counter += 1

                # Calculate P&L (simplified - actual P&L calculated on position close)
                commission = order.broker_metadata.get("commission", 0.0)
                slippage = order.broker_metadata.get("slippage", 0.0)

                trade = TradeRecord(
                    trade_id=order_id,
                    symbol=order.symbol,
                    side=order.side,
                    entry_time=order.submitted_at,
                    entry_price=order.average_fill_price,
                    quantity=order.filled_quantity,
                    exit_time=order.filled_at,
                    exit_price=order.average_fill_price,
                    pnl=0.0,  # P&L calculated when position is closed
                    commission=commission,
                    slippage=abs(slippage) * order.filled_quantity,
                )

                self.trades.append(trade)

    def _calculate_results(self, execution_time: float) -> BacktestResult:
        """
        Calculate comprehensive performance metrics.

        Args:
            execution_time: Total execution time in seconds

        Returns:
            BacktestResult with all performance metrics
        """
        account = self.broker.get_account()

        # Calculate returns
        total_return, total_return_pct = calculate_total_return(
            self.config.initial_capital, account.portfolio_value
        )

        # Calculate risk metrics
        sharpe = calculate_sharpe_ratio(self.equity_curve)
        max_dd, max_dd_pct = calculate_max_drawdown(self.equity_curve)

        # Calculate trade statistics
        # Update trade P&L based on actual positions
        self._calculate_trade_pnl()

        winning_trades = sum(1 for t in self.trades if t.pnl > 0)
        losing_trades = sum(1 for t in self.trades if t.pnl < 0)
        win_rate = calculate_win_rate(self.trades)

        result = BacktestResult(
            config=self.config,
            initial_capital=self.config.initial_capital,
            final_capital=account.portfolio_value,
            total_return=total_return,
            total_return_percent=total_return_pct,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            max_drawdown_percent=max_dd_pct,
            total_trades=len(self.trades),
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            equity_curve=self.equity_curve,
            trades=self.trades,
            execution_time_seconds=execution_time,
        )

        logger.info(
            f"Performance Summary:\n"
            f"  Total Return: ${total_return:,.2f} ({total_return_pct:.2f}%)\n"
            f"  Sharpe Ratio: {sharpe:.2f}\n"
            f"  Max Drawdown: ${max_dd:,.2f} ({max_dd_pct:.2f}%)\n"
            f"  Total Trades: {len(self.trades)}\n"
            f"  Win Rate: {win_rate:.2f}%"
        )

        return result

    def _calculate_trade_pnl(self) -> None:
        """
        Calculate realized P&L for trades based on position changes.

        This is a simplified implementation that estimates P&L based on
        trade direction and price changes. In production, this would track
        position lifecycles more precisely.
        """
        # Group trades by symbol
        trades_by_symbol = defaultdict(list)
        for trade in self.trades:
            trades_by_symbol[trade.symbol].append(trade)

        # Calculate P&L for each symbol's trades
        for symbol, symbol_trades in trades_by_symbol.items():
            position_qty = 0.0
            position_cost = 0.0

            for trade in sorted(symbol_trades, key=lambda t: t.entry_time):
                if trade.side == OrderSide.BUY:
                    # Opening or adding to long position
                    if position_qty >= 0:
                        position_cost += trade.entry_price * trade.quantity
                        position_qty += trade.quantity
                    else:
                        # Closing short position
                        close_qty = min(trade.quantity, abs(position_qty))
                        avg_short_price = (
                            position_cost / abs(position_qty)
                            if position_qty != 0
                            else 0
                        )
                        trade.pnl = (
                            avg_short_price - trade.entry_price
                        ) * close_qty - trade.commission

                        position_qty += close_qty
                        position_cost = (
                            (abs(position_qty) * avg_short_price)
                            if position_qty < 0
                            else 0
                        )

                        # If there's remaining quantity, it opens a new long position
                        if trade.quantity > close_qty:
                            remaining_qty = trade.quantity - close_qty
                            position_cost = trade.entry_price * remaining_qty
                            position_qty = remaining_qty

                else:  # SELL
                    # Closing or reducing long position, or opening short
                    if position_qty > 0:
                        # Closing long position
                        close_qty = min(trade.quantity, position_qty)
                        avg_long_price = position_cost / position_qty
                        trade.pnl = (
                            trade.entry_price - avg_long_price
                        ) * close_qty - trade.commission

                        position_qty -= close_qty
                        position_cost = (
                            position_qty * avg_long_price if position_qty > 0 else 0
                        )

                        # If there's remaining quantity, it opens a new short position
                        if trade.quantity > close_qty:
                            remaining_qty = trade.quantity - close_qty
                            position_cost = trade.entry_price * remaining_qty
                            position_qty = -remaining_qty
                    else:
                        # Opening or adding to short position
                        position_cost += trade.entry_price * trade.quantity
                        position_qty -= trade.quantity


class BacktestContext:
    """
    Simplified context object for backtesting.

    This is a minimal context implementation for the backtest engine.
    In production, this would be replaced by the full StrategyContext
    from Module 3 of the architecture.

    Attributes:
        timestamp: Current timestamp
        bars: Dictionary of current bars by symbol
        broker: Simulated broker instance
        data_loader: Data loader for historical queries
    """

    def __init__(
        self,
        timestamp: datetime,
        bars: Dict[str, OHLCBar],
        broker: SimulatedBroker,
        data_loader: OHLCDataLoader,
    ):
        self.timestamp = timestamp
        self._bars = bars
        self._broker = broker
        self._data_loader = data_loader

    def close(self, symbol: str) -> float:
        """Get current close price for symbol."""
        bar = self._bars.get(symbol)
        return bar.close if bar else None

    def open(self, symbol: str) -> float:
        """Get current open price for symbol."""
        bar = self._bars.get(symbol)
        return bar.open if bar else None

    def high(self, symbol: str) -> float:
        """Get current high price for symbol."""
        bar = self._bars.get(symbol)
        return bar.high if bar else None

    def low(self, symbol: str) -> float:
        """Get current low price for symbol."""
        bar = self._bars.get(symbol)
        return bar.low if bar else None

    def volume(self, symbol: str) -> int:
        """Get current volume for symbol."""
        bar = self._bars.get(symbol)
        return bar.volume if bar else None

    def buy(self, symbol: str, quantity: float):
        """Place a buy order."""
        from ..models import OrderRequest, OrderType, OrderSide

        order = OrderRequest(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=quantity,
        )
        return self._broker.submit_order(order)

    def sell(self, symbol: str, quantity: float):
        """Place a sell order."""
        from ..models import OrderRequest, OrderType, OrderSide

        order = OrderRequest(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=quantity,
        )
        return self._broker.submit_order(order)

    def position(self, symbol: str):
        """Get current position for symbol."""
        return self._broker.get_position(symbol)

    def account(self):
        """Get account information."""
        return self._broker.get_account()
