import logging
import os
from uuid import UUID

from sqlalchemy import update

from config import BASE_PATH
from core.enums import BacktestStatus
from db_models import Backtests, Strategies
from engine.backtesting import BacktestConfig, BacktestEngine
from engine.enums import Timeframe
from utils.db import get_db_sess_sync
from .base_runner import BaseRunner

logger = logging.getLogger(__name__)


class BacktestRunner(BaseRunner):
    """
    Performs a backtest for a given backtest_id.
    """

    def __init__(self, backtest_id: str | UUID):
        self.backtest_id = UUID(str(backtest_id))

    def run(self) -> None:
        """The main entry point for the backtest process."""
        logger.info(f"Starting BacktestRunner for ID '{self.backtest_id}'")

        db_backtest = None
        db_strategy = None

        with get_db_sess_sync() as db_sess:
            db_backtest = db_sess.get(Backtests, self.backtest_id)
            if db_backtest is None:
                logger.error(f"Backtest object not found for ID: {self.backtest_id}")
                return

            logger.info("Backtest object found")

            db_strategy = db_sess.get(Strategies, db_backtest.strategy_id)
            if db_strategy is None:
                logger.error(
                    f"Strategy for backtest {self.backtest_id} not found with ID: {db_backtest.strategy_id}"
                )
                db_backtest.status = BacktestStatus.FAILED
                db_sess.commit()
                return

            logger.info("Strategy object found")

            db_sess.expunge(db_backtest)
            db_sess.expunge(db_strategy)

        temp_strategy_path = os.path.join(BASE_PATH, "user_strategy.py")

        with open(temp_strategy_path, "w") as f:
            f.write(db_strategy.code)
        logger.info(f"Strategy code written to {temp_strategy_path}")

        from user_strategy import Strategy  # type: ignore

        bt_config = BacktestConfig(
            start_date=db_backtest.start_date,
            end_date=db_backtest.end_date,
            symbol=db_backtest.symbol,
            starting_balance=db_backtest.starting_balance,
            timeframe=Timeframe(db_backtest.timeframe),
        )

        strategy = Strategy()

        bt = BacktestEngine(bt_config, strategy)
        result = bt.run()

        logger.info(f"Backtest {self.backtest_id} completed. Result: {result}")

        with get_db_sess_sync() as db_sess:
            db_sess.execute(
                update(Backtests)
                .where(Backtests.backtest_id == self.backtest_id)
                .values(
                    status=BacktestStatus.COMPLETED,
                    metrics=result.model_dump(mode="json", exclude={"config"}),
                )
            )
            db_sess.commit()
            logger.info(f"Metrics updated for backtest {self.backtest_id}")
