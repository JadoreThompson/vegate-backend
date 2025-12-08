import logging
import os
import sys

from sqlalchemy import update
from config import BASE_PATH
from db_models import Backtests, Strategies
from engine.backtesting import BacktestConfig, BacktestEngine
from utils.db import get_db_sess_sync


logger = logging.getLogger(__name__)

BACKTEST_ID = os.getenv("BACKTEST_ID")

if not BACKTEST_ID:
    logger.error("Backtest id not found")
    sys.exit(1)

logger.info(f"Backtest id found '{BACKTEST_ID}'")

with get_db_sess_sync() as db_sess:
    db_backtest = db_sess.get(Backtests, BACKTEST_ID)
    if db_backtest is None:
        logger.info("Backtest object not found")
        sys.exit(2)

    logger.info("Backtest object found")

    db_strategy = db_sess.get(Strategies, db_backtest.strategy_id)
    if db_strategy is None:
        logger.info("Strategy for backtest not found")
        sys.exit(3)

    logger.info("Strategy object found")

    db_sess.expunge(db_backtest)
    db_sess.expunge(db_strategy)


with open(os.path.join(BASE_PATH, "user_strategy.py"), "w") as f:
    f.write(db_strategy.code)


from engine.brokers import BacktestBroker
from engine.enums import Timeframe
from engine.strategy import StrategyManager
from user_strategy import Strategy # type: ignore


bt_config = BacktestConfig(
    start_date=db_backtest.start_date,
    end_date=db_backtest.end_date,
    symbol=db_backtest.symbol,
    starting_balance=db_backtest.starting_balance,
    timeframe=Timeframe(db_backtest.timeframe),
)

broker = BacktestBroker(bt_config.starting_balance)
strategy = Strategy()
manager = StrategyManager(strategy, broker)

bt = BacktestEngine(bt_config, strategy)
result = bt.run()

with get_db_sess_sync() as db_sess:
    db_sess.execute(
        update(Backtests)
        .where(Backtests.backtest_id == db_backtest.backtest_id)
        .values(metrics=result.model_dump(mode="json", exclude={"config"}))
    )
    db_sess.commit()
