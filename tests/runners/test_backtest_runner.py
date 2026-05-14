import uuid
from datetime import date, datetime, UTC
from typing import Generator

import pytest
from unittest.mock import MagicMock, patch

from sqlalchemy import delete, insert, select

from enums import (
    BacktestStatus,
    BrokerType,
    OrderSide,
    OrderStatus,
    OrderType,
    Timeframe,
)
from infra.db.model.backtest import Backtest
from infra.db.model.backtest_equity_curve import BacktestEquityCurve
from infra.db.model.backtest_metric import BacktestMetrics
from infra.db.model.backtest_order import BacktestOrder
from infra.db.model.ohlc import OHLC
from infra.db.model.strategy import Strategy
from infra.db.model.user import User
from infra.db.utils import get_db_sess_sync
from runners.backtest_runner import BacktestRunner


@pytest.fixture
def backtest_id():
    return uuid.uuid4()


class TestBacktestRunnerUnitTest:

    def test_backtest_status_set(self, backtest_id):
        """
        Tests that the backtest status is set to in_progress on enter
        and failed when an error is thrown
        """
        with (
            patch("runners.backtest_runner.get_db_sess_sync") as mock_get_db_sess_sync,
            patch("runners.backtest_runner.BacktestConfig") as MockBacktestConfig,
            patch("runners.backtest_runner.BacktestBroker") as MockBacktestBroker,
        ):
            mock_db_sess = MagicMock()
            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_db_sess
            mock_context_manager.__exit__.return_value = None
            mock_get_db_sess_sync.return_value = mock_context_manager

            runner = BacktestRunner(backtest_id)

            mock_fetch_backtest_and_strategy = MagicMock()

            backtest_entity = Backtest(
                id=uuid.uuid4(),
                strategy_id=uuid.uuid4(),
                symbol="AAPL",
                broker=BrokerType.ALPACA,
                starting_balance=10_000,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
                timeframe=Timeframe.m1,
                status=BacktestStatus.PENDING.value,
                created_at=datetime.now(UTC),
            )

            mock_strategy_entity = MagicMock()
            mock_fetch_backtest_and_strategy.return_value = (
                backtest_entity,
                mock_strategy_entity,
            )
            runner._fetch_backtest_and_strategy = mock_fetch_backtest_and_strategy

            mock_update_backtest_status = MagicMock()
            mock_update_backtest_status.side_effect = [Exception("Side effect"), None]
            runner._update_backtest_status = mock_update_backtest_status

            mock_backtest_config = MagicMock()
            MockBacktestConfig.return_value = mock_backtest_config

            mock_backtest_broker = MagicMock()
            MockBacktestBroker.return_value = mock_backtest_broker

            runner._write_strategy_code = MagicMock()
            runner._load_user_strategy = MagicMock()

            runner.run()

        assert mock_update_backtest_status.call_count == 2
        assert (
            mock_update_backtest_status.call_args_list[0].args[0]
            == BacktestStatus.IN_PROGRESS
        )
        assert (
            mock_update_backtest_status.call_args_list[1].args[0]
            == BacktestStatus.FAILED
        )


class TestBacktestRunnerIT:

    @pytest.fixture
    def seed_backtest(self) -> Generator[tuple[uuid.UUID, list[OHLC]], None, None]:
        user_id = None
        strategy_id = None
        backtest_id = None
        symbol = "APPL"
        broker = BrokerType.ALPACA
        start_date = datetime(2024, 1, 1, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC)

        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(
                insert(User)
                .values(
                    username="test-user",
                    email="test-user@email.com",
                    password="password",
                )
                .returning(User.user_id)
            )
            user_id = res.scalar()

            res = db_sess.execute(
                insert(Strategy)
                .values(
                    user_id=user_id,
                    name="test-strategy",
                    prompt="",
                    code="""
from lib.strategy import BaseStrategy
from models import OrderRequest, Order
from enums import OrderType, OrderSide

class Strategy(BaseStrategy):

    def __init__(self, name, broker):
        super().__init__(name, broker)
        self._order: Order = None

    def on_candle(self, candle):
        if self._order is None:
            self._order = self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY,
                    notional=candle.close,
                )
            )
        else:
            self.broker.place_order(
                OrderRequest(
                    symbol=candle.symbol,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    quantity=self._order.quantity,
                )
            )
            self._order = None
""",
                )
                .returning(Strategy.strategy_id)
            )
            strategy_id = res.scalar()

            res = db_sess.execute(
                insert(Backtest)
                .values(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    broker=broker,
                    timeframe=Timeframe.m1,
                    starting_balance=10_000,
                    start_date=start_date,
                    end_date=end_date,
                )
                .returning(Backtest.id)
            )
            backtest_id = res.scalar()

            candles = [
                OHLC(
                    open=99.0,
                    high=104.0,
                    low=94.0,
                    close=101.0 + i,
                    source=broker,
                    symbol=symbol,
                    timeframe=Timeframe.m1,
                    timestamp=int(
                        datetime(
                            year=start_date.year,
                            month=start_date.month,
                            day=start_date.day,
                            hour=start_date.hour,
                            minute=i + 1,
                        ).timestamp()
                    ),
                )
                for i in range(4)
            ]
            db_sess.add_all(candles)

            db_sess.commit()

        yield backtest_id, candles

        with get_db_sess_sync() as db_sess:
            db_sess.execute(delete(User).where(User.user_id == user_id))
            db_sess.execute(delete(Strategy).where(Strategy.strategy_id == strategy_id))
            db_sess.execute(delete(Backtest).where(Backtest.id == backtest_id))
            db_sess.execute(
                delete(BacktestMetrics).where(
                    BacktestMetrics.backtest_id == backtest_id
                )
            )
            db_sess.execute(
                delete(BacktestOrder).where(BacktestOrder.backtest_id == backtest_id)
            )
            db_sess.execute(
                delete(OHLC).where(OHLC.symbol == symbol, OHLC.source == broker.value)
            )
            db_sess.commit()

    def test_integration(self, seed_backtest):
        backtest_id, candles = seed_backtest

        runner = BacktestRunner(backtest_id)
        runner.run()

        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(select(Backtest).where(Backtest.id == backtest_id))
            db_backtest: Backtest = res.scalar()

            res = db_sess.execute(
                select(BacktestMetrics).where(
                    BacktestMetrics.backtest_id == backtest_id
                )
            )
            db_metrics: BacktestMetrics = res.scalar()

            res = db_sess.execute(
                select(BacktestOrder).where(BacktestOrder.backtest_id == backtest_id)
            )
            db_backtest_orders: list[BacktestOrder] = res.scalars().all()

        assert db_backtest.status == BacktestStatus.COMPLETED

        assert db_metrics.realised_pnl == 2.0
        assert db_metrics.unrealised_pnl == 0.0
        assert db_metrics.total_return_pct == 0.02
        assert db_metrics.profit_factor == float("inf")
        assert db_metrics.total_orders == 4

        assert len(db_backtest_orders) == 4
        assert db_backtest_orders[0].side == OrderSide.BUY
        assert db_backtest_orders[0].symbol == db_backtest.symbol
        assert db_backtest_orders[0].order_type == OrderType.MARKET
        assert db_backtest_orders[0].quantity == 1
        assert db_backtest_orders[0].filled_quantity == 1
        assert db_backtest_orders[0].avg_fill_price == candles[0].close
        assert db_backtest_orders[0].status == OrderStatus.FILLED
        assert db_backtest_orders[0].submitted_at == datetime.fromtimestamp(
            candles[0].timestamp, tz=UTC
        )
        assert db_backtest_orders[0].filled_at == datetime.fromtimestamp(
            candles[0].timestamp, tz=UTC
        )

    def test_integration_engine_exception_no_residual_data(self, seed_backtest):
        """
        If the backtest engine throws during run(), the runner should mark the
        backtest as FAILED and must not leave behind any metrics, orders, or
        equity-curve rows.
        """
        backtest_id, candles = seed_backtest

        with patch(
            "runners.backtest_runner.BacktestRunner._store_results"
        ) as mock_store_results:
            mock_store_results.side_effect = Exception("side effect")
            runner = BacktestRunner(backtest_id)
            runner.run()

        with get_db_sess_sync() as db_sess:
            res = db_sess.execute(select(Backtest).where(Backtest.id == backtest_id))
            db_backtest: Backtest = res.scalar()

            res = db_sess.execute(
                select(BacktestMetrics).where(
                    BacktestMetrics.backtest_id == backtest_id
                )
            )
            db_metrics = res.scalar()

            res = db_sess.execute(
                select(BacktestOrder).where(BacktestOrder.backtest_id == backtest_id)
            )
            db_orders: list[BacktestOrder] = res.scalars().all()

            res = db_sess.execute(
                select(BacktestEquityCurve).where(
                    BacktestEquityCurve.backtest_id == backtest_id
                )
            )
            db_equity_curve = res.scalars().all()

        assert db_backtest.status == BacktestStatus.FAILED
        assert db_metrics is None
        assert len(db_orders) == 0
        assert len(db_equity_curve) == 0
