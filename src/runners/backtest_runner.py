import logging
import os
from uuid import UUID

from sqlalchemy import insert, update

from config import BASE_PATH
from core.enums import BacktestStatus
from infra.db.models import Backtests, Orders, Strategies
from engine.backtesting import BacktestConfig, BacktestEngine
from engine.enums import Timeframe
from infra.db import get_db_sess_sync
from .base import BaseRunner




class BacktestRunner(BaseRunner):
    """
    Performs a backtest for a given backtest_id.
    """

    def __init__(self, backtest_id: str | UUID):
        self._backtest_id = UUID(str(backtest_id))
        self._logger = logging.getLogger(type(self).__name__)

    def run(self) -> None:
        """The main entry point for the backtest process."""
        self._logger.info(f"Starting BacktestRunner for ID '{self._backtest_id}'")

        db_backtest = None
        db_strategy = None
        metrics = None

        try:
            with get_db_sess_sync() as db_sess:
                db_backtest = db_sess.get(Backtests, self._backtest_id)
                if db_backtest is None:
                    self._logger.error(
                        f"Backtest object not found for ID: {self._backtest_id}"
                    )
                    return

                self._logger.info("Backtest object found")

                db_strategy = db_sess.get(Strategies, db_backtest.strategy_id)
                if db_strategy is None:
                    self._logger.error(
                        f"Strategy for backtest {self._backtest_id} not found with ID: {db_backtest.strategy_id}"
                    )
                    db_backtest.status = BacktestStatus.FAILED
                    db_sess.commit()
                    return

                self._logger.info("Strategy object found")

                db_sess.expunge(db_backtest)
                db_sess.expunge(db_strategy)

            temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")

            with open(temp_strategy_path, "w") as f:
                f.write(db_strategy.code)
            self._logger.info(f"Strategy code written to {temp_strategy_path}")

            from user_strategy import Strategy  # type: ignore

            bt_config = BacktestConfig(
                start_date=db_backtest.start_date,
                end_date=db_backtest.end_date,
                symbol=db_backtest.symbol,
                starting_balance=db_backtest.starting_balance,
                timeframe=Timeframe(db_backtest.timeframe),
            )

            strategy = Strategy()
            bt = BacktestEngine(strategy, bt_config)
            result = bt.run()

            self._logger.info(f"Backtest {self._backtest_id} completed")

            records = []
            for order in result.orders:
                o = order.model_dump(mode="json", exclude={"broker_metadata"})
                o["backtest_id"] = self._backtest_id
                o["symbol"] = bt_config.symbol
                records.append(o)

            equity_curve = result.equity_curve
            n = len(equity_curve)
            if n > 5:
                indices = [0, n * 1 // 4, n * 2 // 4, n * 3 // 4, n - 1]
                equity_curve = [equity_curve[i] for i in indices]

            result.equity_curve = equity_curve
            metrics = result.model_dump(mode="json", exclude={"config"})

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

        except Exception as e:
            self._logger.error(
                f"An error occured handling backtest {self._backtest_id}", exc_info=e
            )
            with get_db_sess_sync() as db_sess:
                db_sess.execute(
                    update(Backtests)
                    .where(Backtests.backtest_id == self._backtest_id)
                    .values(
                        status=BacktestStatus.FAILED,
                        metrics=metrics,
                    )
                )
                db_sess.commit()
        
