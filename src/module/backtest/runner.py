import logging
import os
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import insert, update

from config import SRC_PATH
from core.db import get_db_sess_sync
from module.backtest.engine import BacktestEngine
from module.backtest.enums import BacktestStatus
from module.event_bus import SyncEventPublisher
from module.strategy.model import Strategy
from module.strategy.strategy import BaseStrategy
from .engine.schema import BacktestMetrics as BacktestMetricsDto
from .model import Backtest, BacktestMetrics, BacktestOrder
from .ohlc_feed_client import BacktestOHLCFeedClient
from .oms_client import BacktestOMSClient


class BacktestRunner:
    """Performs a backtest for a given backtest_id."""

    def __init__(self, backtest_id: UUID):
        self._backtest_id = backtest_id
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """The main entry point for the backtest process."""
        self._logger.info(f"Starting BacktestRunner for ID '{self._backtest_id}'")

        try:

            with get_db_sess_sync() as db_sess:
                db_backtest = db_sess.get(Backtest, self._backtest_id)
                if db_backtest is None:
                    self._logger.error(
                        f"Backtest object not found for ID: {self._backtest_id}"
                    )
                    return

                if db_backtest.status == BacktestStatus.IN_PROGRESS:
                    self._logger.info(
                        f"Backtest is already in progress. Abandoning backtest"
                    )
                    return

                if db_backtest.status == BacktestStatus.COMPLETED:
                    self._logger.info(
                        f"Backtest is already complete. Abandoning backtest"
                    )
                    return

                db_backtest.status = BacktestStatus.IN_PROGRESS
                self._logger.info("Backtest object found")
                db_sess.flush()
                db_sess.expunge(db_backtest)

                db_strategy = db_sess.get(Strategy, db_backtest.strategy_id)
                if db_strategy is None:
                    self._logger.error(f"Strategy for backtest {self._backtest_id}")
                    return

                self._logger.info("Strategy object found")
                db_sess.expunge(db_strategy)

                db_sess.commit()

            # Create broker and run backtest
            self._write_strategy_code(db_strategy.code)

            ohlc_feed_client = BacktestOHLCFeedClient(
                int(db_backtest.start_date.timestamp()),
                int(db_backtest.end_date.timestamp()),
            )
            oms_client = BacktestOMSClient(db_backtest.starting_balance)
            strategy = self._load_user_strategy(ohlc_feed_client, oms_client)

            bt_engine = BacktestEngine(
                strategy,
                db_backtest.starting_balance,
                db_backtest.start_date,
                db_backtest.end_date,
            )
            result = bt_engine.run()
            self._logger.info(f"Backtest {self._backtest_id} completed")

            # Store results to database
            self._logger.info("Storing results...")
            self._store_results(result)
            self._logger.info("Finished storing results")

        except Exception as e:
            self._logger.error(
                f"An error occurred handling backtest {self._backtest_id}", exc_info=e
            )
            self._update_backtest_status(BacktestStatus.FAILED)

    def _write_strategy_code(self, code: str) -> None:
        """Write strategy code to user_strategy.py file.

        Args:
            code: Strategy code to write
        """
        temp_strategy_path = os.path.join(SRC_PATH, "user_strategy.py")
        with open(temp_strategy_path, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {temp_strategy_path}")

    def _load_user_strategy(
        self, ohlc_feed_client: BacktestOHLCFeedClient, oms_client: BacktestOMSClient
    ) -> BaseStrategy:
        """Load and instantiate strategy from user_strategy.py.

        Returns:
            Strategy instance
        """
        from user_strategy import UserStrategy  # type: ignore

        event_publisher = SyncEventPublisher()

        return UserStrategy(
            ohlc_feed_client=ohlc_feed_client,
            oms_client=oms_client,
            event_publisher=event_publisher,
        )
    
    def _store_results(self, result: BacktestMetricsDto) -> None:
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
            o["filled_at"] = o["executed_at"]
            records.append(o)

        # Downsample equity curve if too large
        equity_curve = result.equity_curve
        n = len(equity_curve)
        if n > 5:
            indices = [0, n * 1 // 4, n * 2 // 4, n * 3 // 4, n - 1]
            equity_curve = [asdict(equity_curve[i]) for i in indices]

        # Update database
        with get_db_sess_sync() as db_sess:
            if records:
                db_sess.execute(insert(BacktestOrder), records)

            db_sess.execute(
                update(Backtest)
                .where(Backtest.id == self._backtest_id)
                .values(status=BacktestStatus.COMPLETED)
            )

            db_sess.execute(
                insert(BacktestMetrics).values(
                    backtest_id=self._backtest_id,
                    realised_pnl=result.realised_pnl,
                    unrealised_pnl=result.unrealised_pnl,
                    total_return_pct=result.total_return_pct,
                    profit_factor=result.profit_factor,
                    total_orders=result.total_orders,
                    equity_curve=equity_curve,
                )
            )

            db_sess.commit()
            self._logger.info(f"Metrics updated for backtest {self._backtest_id}")

    def _update_backtest_status(self, status: BacktestStatus) -> None:
        """Update backtest status in database.

        Args:
            status: BacktestStatus to set
        """
        with get_db_sess_sync() as db_sess:
            db_sess.execute(
                update(Backtest)
                .where(
                    Backtest.id == self._backtest_id,
                    Backtest.status != BacktestStatus.COMPLETED,
                )
                .values(status=status.value)
            )
            db_sess.commit()
