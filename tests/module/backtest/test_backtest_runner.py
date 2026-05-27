import time
from threading import Thread
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from config import REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX
from module.backtest.runner import BacktestRunner


@pytest.fixture
def mock_redis_client():
    return MagicMock()


@pytest.fixture
def mock_event_publisher():
    return MagicMock()


@pytest.fixture
def runner(mock_event_publisher, mock_redis_client):
    return BacktestRunner(
        backtest_id=uuid4(),
        event_publisher=mock_event_publisher,
        redis_client=mock_redis_client,
        heartbeat_interval=1,
    )


class TestBacktestRunnerHeartbeat:

    def test_heartbeat_loop_sets_redis_key(self, runner, mock_redis_client):
        runner._is_running = True

        thread = Thread(target=runner._heartbeat_loop, daemon=True)
        thread.start()

        time.sleep(0.5)

        runner._is_running = False
        thread.join(timeout=2)

        expected_key = f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{runner._backtest_id}"
        mock_redis_client.set.assert_called()
        args, kwargs = mock_redis_client.set.call_args
        assert args[0] == expected_key
        assert kwargs == {"ex": 15}

    def test_heartbeat_loop_stops_when_not_running(self, runner, mock_redis_client):
        runner._is_running = False

        runner._heartbeat_loop()

        mock_redis_client.set.assert_not_called()

    def test_heartbeat_loop_multiple_heartbeats(self, runner, mock_redis_client):
        runner._is_running = True

        thread = Thread(target=runner._heartbeat_loop, daemon=True)
        thread.start()

        time.sleep(2.5)

        runner._is_running = False
        thread.join(timeout=2)

        assert mock_redis_client.set.call_count >= 2

        expected_key = f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{runner._backtest_id}"
        for call_args in mock_redis_client.set.call_args_list:
            args, kwargs = call_args
            assert args[0] == expected_key
            assert kwargs == {"ex": 15}

    def test_heartbeat_loop_uses_correct_ttl(self, runner, mock_redis_client):
        runner._is_running = True

        thread = Thread(target=runner._heartbeat_loop, daemon=True)
        thread.start()

        time.sleep(1.5)

        runner._is_running = False
        thread.join(timeout=2)

        expected_key = f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{runner._backtest_id}"
        _, kwargs = mock_redis_client.set.call_args
        assert kwargs == {"ex": 15}


MODULE_PATH = "module.backtest.runner"


def _make_mock_backtest(backtest_id, strategy_id, start_ts, end_ts):
    mock_backtest = MagicMock()
    mock_backtest.id = backtest_id
    mock_backtest.strategy_id = strategy_id
    mock_backtest.starting_balance = 10000
    mock_backtest.start_date = start_ts
    mock_backtest.end_date = end_ts
    mock_backtest.status = "pending"
    return mock_backtest


def _make_mock_strategy(strategy_id):
    mock_strategy = MagicMock()
    mock_strategy.strategy_id = strategy_id
    mock_strategy.code = """
from module.strategy.strategy import BaseStrategy

class UserStrategy(BaseStrategy):
    def on_candle(self, candle):
        pass
"""
    return mock_strategy


def _make_mock_strategy_version(version_id):
    mock_strategy = MagicMock()
    mock_strategy.id = version_id
    mock_strategy.code = """
from module.strategy.strategy import BaseStrategy

class UserStrategy(BaseStrategy):
    def on_candle(self, candle):
        pass
"""
    return mock_strategy


class TestBacktestRunnerWithRealRun:

    def test_run_starts_heartbeat_and_performs_it(
        self, mock_redis_client, mock_event_publisher
    ):
        """
        Runs the full BacktestRunner.run() with mocked DB and engine,
        verifying that the heartbeat loop fires Redis set calls during execution.
        """
        from module.backtest.model import Backtest as BacktestModel
        from module.strategy.model import StrategyVersion

        backtest_id = uuid4()
        strategy_id = uuid4()
        start_ts = MagicMock()
        start_ts.timestamp.return_value = 1000000
        end_ts = MagicMock()
        end_ts.timestamp.return_value = 2000000

        mock_backtest = _make_mock_backtest(backtest_id, strategy_id, start_ts, end_ts)
        mock_strategy = _make_mock_strategy(strategy_id)
        mock_strategy_version = _make_mock_strategy_version(uuid4())

        mock_db_sess = MagicMock()
        mock_db_sess.get.side_effect = lambda model, pk: (
            mock_backtest
            if model == BacktestModel
            else
            # mock_strategy if model == StrategyModel else
            mock_strategy_version if model == StrategyVersion else None
        )
        mock_db_sess.commit = MagicMock(return_value=None)
        mock_db_sess.flush = MagicMock(return_value=None)
        mock_db_sess.expunge = MagicMock(return_value=None)

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db_sess_sync:
            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_db_sess
            mock_get_db_sess_sync.return_value = mock_context_manager

            with patch(f"{MODULE_PATH}.BacktestEngine") as MockBacktestEngine:
                mock_result = MagicMock()
                mock_result.orders = []
                mock_result.equity_curve = []
                mock_result.realised_pnl = 0.0
                mock_result.unrealised_pnl = 0.0
                mock_result.total_return_pct = 0.0
                mock_result.profit_factor = 0.0
                mock_result.total_orders = 0
                mock_engine = MagicMock()
                mock_engine.run.side_effect = lambda: time.sleep(1) or mock_result
                MockBacktestEngine.return_value = mock_engine

                runner = BacktestRunner(
                    backtest_id=backtest_id,
                    event_publisher=mock_event_publisher,
                    redis_client=mock_redis_client,
                    heartbeat_interval=0.1,
                )

                with (
                    patch.object(runner, "_write_strategy_code"),
                    patch.object(runner, "_load_user_strategy") as mock_load,
                ):
                    mock_load.return_value = MagicMock()
                    runner.run()

        expected_key = f"{REDIS_BACKTEST_HEARTBEAT_KEY_PREFIX}{backtest_id}"
        # mock_redis_client.set.assert_called()
        assert (
            mock_redis_client.set.call_count >= 1
        ), "Expected at least one heartbeat set call"
        args, kwargs = mock_redis_client.set.call_args
        assert args[0] == expected_key
        assert kwargs == {"ex": 15}

    def test_cancellation_emits_failed_event(
        self, mock_redis_client, mock_event_publisher
    ):
        """
        Verifies that when is_running is set to False during execution,
        the runner emits a FAILED event.
        """
        from module.backtest.model import Backtest as BacktestModel
        from module.strategy.model import Strategy as StrategyModel, StrategyVersion

        backtest_id = uuid4()
        strategy_id = uuid4()
        start_ts = MagicMock()
        start_ts.timestamp.return_value = 1000000
        end_ts = MagicMock()
        end_ts.timestamp.return_value = 2000000

        mock_backtest = _make_mock_backtest(backtest_id, strategy_id, start_ts, end_ts)
        mock_strategy = _make_mock_strategy(strategy_id)
        mock_strategy_version = _make_mock_strategy_version(uuid4())

        mock_db_sess = MagicMock()
        mock_db_sess.get.side_effect = lambda model, pk: (
            mock_backtest
            if model == BacktestModel
            # else mock_strategy if model == StrategyModel else None
            else mock_strategy_version if model == StrategyVersion else None
        )
        mock_db_sess.commit = MagicMock(return_value=None)
        mock_db_sess.flush = MagicMock(return_value=None)
        mock_db_sess.expunge = MagicMock(return_value=None)

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db_sess_sync:
            mock_context_manager = MagicMock()
            mock_context_manager.__enter__.return_value = mock_db_sess
            mock_get_db_sess_sync.return_value = mock_context_manager

            def candles_generator():
                for _ in range(5):
                    runner._is_running = False
                    yield MagicMock()

            with patch(
                f"{MODULE_PATH}.BacktestOHLCFeedClient"
            ) as MockBacktestOHLCFeedClient:
                mock_feed_client = MagicMock()
                mock_feed_client.candles.return_value = candles_generator()
                MockBacktestOHLCFeedClient.return_value = mock_feed_client

                runner = BacktestRunner(
                    backtest_id=backtest_id,
                    event_publisher=mock_event_publisher,
                    redis_client=mock_redis_client,
                    heartbeat_interval=0.1,
                )

                runner_thread = Thread(target=runner.run, daemon=True)
                runner_thread.start()

                time.sleep(0.15)
                runner._is_running = False

                runner_thread.join(timeout=5)

        failed_events = [
            call_args[0][0]
            for call_args in mock_event_publisher.enqueue.call_args_list
            if (
                hasattr(call_args[0][0], "status")
                and call_args[0][0].status.value == "failed"
            )
        ]

        completed_events = [
            call_args[0][0]
            for call_args in mock_event_publisher.enqueue.call_args_list
            if (
                hasattr(call_args[0][0], "status")
                and call_args[0][0].status.value == "completed"
            )
        ]

        assert len(failed_events) >= 1, "Expected at least one FAILED event"
        assert len(completed_events) == 0, "Expected no COMPLETED events"
