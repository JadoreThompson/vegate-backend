import logging
import os
from datetime import datetime
from uuid import UUID

from sqlalchemy import insert, update

from backtest.config import BacktestConfig
from backtest.engine import BacktestEngine
from config import SRC_PATH
from enums import BacktestStatus, BrokerType, Timeframe
from infra.db import get_db_sess_sync
from infra.db.model import (
    Backtest,
    Strategy as StrategyEntity,
    BacktestOrder,
    BacktestMetrics,
    BacktestEquityCurve,
)
from models import BacktestMetrics as BacktestMetricsModel, EquityCurvePoint
from service.event.publisher import SyncEventPublisher
from service.ohlc.feed.backtest.client import BacktestOHLCFeedClient
from service.oms.broker_client.backtest import BacktestBrokerClient
from strategy.strategy import Strategy
from utils import get_datetime
from .base import BaseRunner


class BacktestRunner(BaseRunner):
    """Performs a backtest for a given backtest_id."""

    def __init__(self, backtest_id: UUID):
        self._backtest_id = backtest_id
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """The main entry point for the backtest process."""
        self._logger.info(f"Starting BacktestRunner for ID '{self._backtest_id}'")

        try:
            # Fetch backtest and strategy from database
            db_backtest, db_strategy = self._fetch_backtest_and_strategy()
            if db_backtest is None or db_strategy is None:
                return
            if db_backtest.status == BacktestStatus.COMPLETED:
                raise ValueError("Backtest already complete")

            self._update_backtest_status(BacktestStatus.IN_PROGRESS)

            # Create backtest configuration
            bt_config = BacktestConfig(
                start_date=db_backtest.start_date,
                end_date=db_backtest.end_date,
                symbol=db_backtest.symbol,
                market_type=db_backtest.market_type,
                starting_balance=db_backtest.starting_balance,
                timeframe=Timeframe(db_backtest.timeframe),
                broker=BrokerType(db_backtest.broker),
            )

            # Create broker and run backtest
            self._write_strategy_code(db_strategy.code)

            strategy = self._load_user_strategy(bt_config)
            bt_engine = BacktestEngine(strategy=strategy, config=bt_config)
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
        finally:
            self._update_backtest_status(BacktestStatus.FAILED)

    def _fetch_backtest_and_strategy(
        self,
    ) -> tuple[Backtest | None, StrategyEntity | None]:
        """Fetch backtest and strategy from database.

        Returns:
            Tuple of (db_backtest, db_strategy) or (None, None) if not found
        """
        with get_db_sess_sync() as db_sess:
            db_backtest = db_sess.get(Backtest, self._backtest_id)
            if db_backtest is None:
                self._logger.error(
                    f"Backtest object not found for ID: {self._backtest_id}"
                )
                return None, None

            self._logger.info("Backtest object found")

            db_strategy = db_sess.get(StrategyEntity, db_backtest.strategy_id)
            if db_strategy is None:
                self._logger.error(
                    f"Strategy for backtest {self._backtest_id}"
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
        temp_strategy_path = os.path.join(SRC_PATH, "user_strategy.py")
        with open(temp_strategy_path, "w") as f:
            f.write(code)
        self._logger.info(f"Strategy code written to {temp_strategy_path}")

    # def _load_strategy(self, name: str, broker: BacktestBroker) -> BaseStrategy:
    def _load_user_strategy(self, config: BacktestConfig) -> Strategy:
        """Load and instantiate strategy from user_strategy.py.

        Returns:
            Strategy instance
        """
        from user_strategy import UserStrategy  # type: ignore

        ohlc_feed_client = BacktestOHLCFeedClient(
            start=int(config.start_date.timestamp()),
            end=int(config.end_date.timestamp()),
        )
        backtest_broker = BacktestBrokerClient(starting_balance=config.starting_balance)
        event_publisher = SyncEventPublisher()

        return UserStrategy(
            config=config,
            ohlc_feed_client=ohlc_feed_client,
            oms_client=backtest_broker,
            event_publisher=event_publisher,
        )

    def _create_backtest_config(self, db_backtest: Backtest) -> BacktestConfig:
        """Create backtest configuration from database backtest.

        Args:
            db_backtest: Database backtest object

        Returns:
            BacktestConfig instance
        """

        return BacktestConfig(
            start_date=db_backtest.start_date,
            end_date=db_backtest.end_date,
            symbol=db_backtest.symbol,
            starting_balance=db_backtest.starting_balance,
            timeframe=Timeframe(db_backtest.timeframe),
            broker=BrokerType(db_backtest.broker),
        )

    def _store_results(self, result: BacktestMetricsModel) -> None:
        """Store backtest results to database.

        Args:
            result: BacktestMetricsModel result
            db_backtest: Database backtest object
            bt_config: BacktestConfig used
        """
        # Prepare order records
        records = []
        for order in result.orders:
            o = order.model_dump(mode="json")
            o["backtest_id"] = self._backtest_id
            o["filled_quantity"] = o["executed_quantity"]
            o["avg_fill_price"] = o["filled_avg_price"]
            o["filled_at"] = o["executed_at"]
            records.append(o)

        # Downsample equity curve if too large
        equity_curve = result.equity_curve
        n = len(equity_curve)
        if n > 5:
            indices = [0, n * 1 // 4, n * 2 // 4, n * 3 // 4, n - 1]
            equity_curve = [equity_curve[i].model_dump(mode='json') for i in indices]

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
                    equity_curve=equity_curve
                )
            )

            def parse_equity_curve_point(
                point: EquityCurvePoint, created_at: datetime
            ) -> dict:
                data = point.model_dump()
                data["backtest_id"] = self._backtest_id
                data["timestamp"] = data["timestamp"].timestamp()
                data["created_at"] = created_at
                return data

            created_at = get_datetime()
            db_sess.execute(
                insert(BacktestEquityCurve),
                [
                    parse_equity_curve_point(point, created_at)
                    for point in result.equity_curve
                ],
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
