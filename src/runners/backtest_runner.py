import logging
import os
from uuid import UUID

from sqlalchemy import insert, update

from config import BASE_PATH
from enums import BacktestStatus, BrokerType
from infra.db import get_db_sess_sync
from infra.db.models import Backtests, Orders, Strategies
from lib.backtest_engine import BacktestConfig, BacktestEngine
from lib.brokers import BacktestBroker
from lib.strategy import BaseStrategy
from models import BacktestMetrics
from .base import BaseRunner


class BacktestRunner(BaseRunner):
    """Performs a backtest for a given backtest_id."""

    def __init__(self, backtest_id: UUID):
        self._backtest_id = backtest_id
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """The main entry point for the backtest process."""
        self._logger.info(f"Starting BacktestRunner for ID '{self._backtest_id}'")

        db_backtest = None
        db_strategy = None
        metrics = None

        try:
            # Fetch backtest and strategy from database
            db_backtest, db_strategy = self._fetch_backtest_and_strategy()
            if db_backtest is None or db_strategy is None:
                return

            # Write strategy code to file
            self._write_strategy_code(db_strategy.code)

            # Import and instantiate strategy
            strategy = self._load_strategy()

            # Create backtest configuration
            bt_config = self._create_backtest_config(db_backtest)

            # Create broker and run backtest
            broker = BacktestBroker(db_backtest.starting_balance)
            engine = BacktestEngine(strategy, broker, bt_config)
            result = engine.run()

            self._logger.info(f"Backtest {self._backtest_id} completed")

            # Store results to database
            self._store_results(result, db_backtest, bt_config)

        except Exception as e:
            self._logger.error(
                f"An error occurred handling backtest {self._backtest_id}", exc_info=e
            )
            self._update_backtest_status(BacktestStatus.FAILED, metrics)

    def _fetch_backtest_and_strategy(
        self,
    ) -> tuple[Backtests | None, Strategies | None]:
        """Fetch backtest and strategy from database.

        Returns:
            Tuple of (db_backtest, db_strategy) or (None, None) if not found
        """
        with get_db_sess_sync() as db_sess:
            db_backtest = db_sess.get(Backtests, self._backtest_id)
            if db_backtest is None:
                self._logger.error(
                    f"Backtest object not found for ID: {self._backtest_id}"
                )
                return None, None

            self._logger.info("Backtest object found")

            db_strategy = db_sess.get(Strategies, db_backtest.strategy_id)
            if db_strategy is None:
                self._logger.error(
                    f"Strategy for backtest {self._backtest_id} not found with ID: {db_backtest.strategy_id}"
                )
                db_backtest.status = BacktestStatus.FAILED.value
                db_sess.commit()
                return None, None

            self._logger.info("Strategy object found")

            # Expunge objects from session to use outside of context
            db_sess.expunge(db_backtest)
            db_sess.expunge(db_strategy)

        return db_backtest, db_strategy

    def _write_strategy_code(self, code: str) -> None:
        """Write strategy code to user_strategy.py file.

        Args:
            code: Strategy code to write
        """
        temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")
        with open(temp_strategy_path, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {temp_strategy_path}")

    def _load_strategy(self) -> BaseStrategy:
        """Load and instantiate strategy from user_strategy.py.

        Returns:
            Strategy instance
        """
        from user_strategy import Strategy  # type: ignore

        return Strategy()

    def _create_backtest_config(self, db_backtest: Backtests) -> BacktestConfig:
        """Create backtest configuration from database backtest.

        Args:
            db_backtest: Database backtest object

        Returns:
            BacktestConfig instance
        """
        # Get broker from backtest server_data or default to ALPACA
        broker_type = BrokerType.ALPACA
        if db_backtest.server_data and "broker" in db_backtest.server_data:
            broker_value = db_backtest.server_data["broker"]
            try:
                broker_type = BrokerType(broker_value)
            except ValueError:
                self._logger.warning(
                    f"Invalid broker type '{broker_value}', defaulting to ALPACA"
                )

        return BacktestConfig(
            start_date=db_backtest.start_date,
            end_date=db_backtest.end_date,
            symbol=db_backtest.symbol,
            starting_balance=db_backtest.starting_balance,
            timeframe=db_backtest.timeframe,
            broker=broker_type,
        )

    def _store_results(
        self, result: BacktestMetrics, db_backtest: Backtests, bt_config: BacktestConfig
    ) -> None:
        """Store backtest results to database.

        Args:
            result: BacktestMetrics result
            db_backtest: Database backtest object
            bt_config: BacktestConfig used
        """
        # Prepare order records
        records = []
        for order in result.orders:
            o = order.model_dump(mode="json")
            o["backtest_id"] = self._backtest_id
            o["symbol"] = bt_config.symbol
            records.append(o)

        # Downsample equity curve if too large
        equity_curve = result.equity_curve
        n = len(equity_curve)
        if n > 5:
            indices = [0, n * 1 // 4, n * 2 // 4, n * 3 // 4, n - 1]
            equity_curve = [equity_curve[i] for i in indices]

        # Convert equity curve points to dictionaries
        equity_curve_data = [
            {"timestamp": point.timestamp.isoformat(), "equity": point.equity}
            for point in equity_curve
        ]

        # Prepare metrics
        metrics = result.model_dump(mode="json")
        metrics["equity_curve"] = equity_curve_data

        # Update database
        with get_db_sess_sync() as db_sess:
            if records:
                db_sess.execute(insert(Orders), records)
            db_sess.execute(
                update(Backtests)
                .where(Backtests.backtest_id == self._backtest_id)
                .values(
                    status=BacktestStatus.COMPLETED.value,
                    metrics=metrics,
                )
            )
            db_sess.commit()
            self._logger.info(f"Metrics updated for backtest {self._backtest_id}")

    def _update_backtest_status(
        self, status: BacktestStatus, metrics: dict | None = None
    ) -> None:
        """Update backtest status in database.

        Args:
            status: BacktestStatus to set
            metrics: Optional metrics to store
        """
        with get_db_sess_sync() as db_sess:
            db_sess.execute(
                update(Backtests)
                .where(Backtests.backtest_id == self._backtest_id)
                .values(
                    status=status.value,
                    metrics=metrics,
                )
            )
            db_sess.commit()
