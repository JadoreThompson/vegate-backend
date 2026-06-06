from dataclasses import asdict
import logging
import time

from threading import Thread
from uuid import UUID

from redis import Redis
from sqlalchemy import insert

from config import HISTORICAL_BASE_URL, REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX
from core.db import get_db_sess_sync
from module.event_bus import SyncEventPublisher
from module.strategy.loader import StrategyLoader
from module.strategy.model import StrategyVersion
from vegate.markets.historical.client import HistoricalDataClient
from vegate.strategy.base import BaseStrategy
from .engine import BacktestEngine
from .engine.ohlc_feed_client import BacktestOHLCFeedClient
from .engine.ohlc_feed_client_proxy import BacktestOHLCFeedClientProxy
from .engine.oms_client import BacktestOMSClient
from .engine.schema import BacktestMetrics as BacktestMetricsDto
from .enums import BacktestStatus
from .event import BacktestStatusChangedEvent
from .model import Backtest, BacktestMetrics, BacktestOrder
from .schema import BacktestMetricsSchema, EquityCurvePoint


class BacktestRunner:
    """Performs a backtest for a given backtest_id."""

    def __init__(
        self,
        backtest_id: UUID,
        event_publisher: SyncEventPublisher,
        redis_client: Redis,
        heartbeat_interval: int = 5,
    ):
        self._backtest_id = backtest_id
        self._event_publisher = event_publisher
        self._redis_client = redis_client
        self._heartbeat_interval = heartbeat_interval
        self._is_running = False
        self._logger = logging.getLogger(type(self).__name__)

    @property
    def is_running(self) -> bool:
        return self._is_running

    def run(self) -> None:
        """The main entry point for the backtest process."""
        self._logger.info(f"Starting BacktestRunner for ID '{self._backtest_id}'")

        self._is_running = True
        heartbeat_thread = Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()

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

                self._logger.info("Backtest object found")
                db_sess.flush()
                db_sess.expunge(db_backtest)

                db_strategy_version = db_sess.get(StrategyVersion, db_backtest.version_id)
                if db_strategy_version is None:
                    self._logger.error(f"Strategy version for backtest {self._backtest_id}")
                    return

                self._logger.info("Strategy version object found")
                db_sess.expunge(db_strategy_version)

            # Create broker and run backtest
            ohlc_feed_client = BacktestOHLCFeedClient(
                int(db_backtest.start_date.timestamp()),
                int(db_backtest.end_date.timestamp()),
            )
            ohlc_feed_client_proxy = BacktestOHLCFeedClientProxy(ohlc_feed_client, self)
            oms_client = BacktestOMSClient(db_backtest.starting_balance)
            historical_data_client = HistoricalDataClient(base_url=HISTORICAL_BASE_URL)
            loader = StrategyLoader(
                ohlc_feed_client_proxy,
                oms_client,
                historical_data_client,
            )
            strategy = loader.load_strategy(db_strategy_version.code)

            bt_engine = BacktestEngine(
                strategy,
                db_backtest.starting_balance,
                db_backtest.start_date,
                db_backtest.end_date,
            )

            self._event_publisher.publish(
                BacktestStatusChangedEvent(
                    backtest_id=self._backtest_id, status=BacktestStatus.IN_PROGRESS
                )
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
            self._event_publisher.publish(
                BacktestStatusChangedEvent(
                    backtest_id=self._backtest_id, status=BacktestStatus.FAILED
                )
            )
        finally:
            self._is_running = False

    def _store_results(self, result: BacktestMetricsDto) -> None:
        """Emit backtest results as events.

        Args:
            result: BacktestMetrics result
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
            equity_curve = [equity_curve[i] for i in indices]
    
        with get_db_sess_sync() as db_sess:
            db_sess.execute(insert(BacktestOrder), records)
            db_sess.execute(insert(BacktestMetrics).values(
                backtest_id=self._backtest_id,
                realised_pnl=result.realised_pnl,
                unrealised_pnl=result.unrealised_pnl,
                total_return_pct=result.total_return_pct,
                profit_factor=result.profit_factor,
                total_orders=result.total_orders,
                equity_curve=[asdict(curve) for curve in equity_curve]
            ))

            self._event_publisher.publish(
                BacktestStatusChangedEvent(
                    backtest_id=self._backtest_id, status=BacktestStatus.COMPLETED
                ),
                db_sess
            )

            db_sess.commit()

    def _heartbeat_loop(self):
        while self._is_running:
            time.sleep(self._heartbeat_interval)
            self._redis_client.set(
                f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{self._backtest_id}",
                int(time.time()),
                ex=15,
            )
