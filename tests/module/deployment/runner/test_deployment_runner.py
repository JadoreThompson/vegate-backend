import time
from threading import Thread
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from config import REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event.event import DeploymentEventType
from module.deployment.runner import StrategyDeploymentRunner

MODULE_PATH = "module.deployment.runner"


@pytest.fixture
def deployment_id():
    return uuid4()


@pytest.fixture
def mock_redis_client():
    return MagicMock()


@pytest.fixture
def mock_event_publisher():
    publisher = MagicMock()
    publisher.publish = MagicMock()
    return publisher


@pytest.fixture
def mock_ohlc_feed_client():
    client = MagicMock()
    client.candles.return_value = iter([])
    return client


@pytest.fixture
def mock_oms_client():
    return MagicMock()


@pytest.fixture
def runner(
    deployment_id,
    mock_ohlc_feed_client,
    mock_oms_client,
    mock_event_publisher,
    mock_redis_client,
):
    return StrategyDeploymentRunner(
        deployment_id=deployment_id,
        ohlc_feed_client=mock_ohlc_feed_client,
        oms_client=mock_oms_client,
        event_publisher=mock_event_publisher,
        redis_client=mock_redis_client,
        heartbeat_interval=1,
    )


class TestHeartbeatLoop:

    def test_heartbeat_loop_sets_redis_key(self, runner, mock_redis_client, deployment_id):
        runner._alive = True

        thread = Thread(target=runner._heartbeat_loop, daemon=True)
        thread.start()
        time.sleep(1.5)

        runner._alive = False
        thread.join(timeout=2)

        expected_key = f"{REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX}{deployment_id}"
        mock_redis_client.set.assert_called()
        args, kwargs = mock_redis_client.set.call_args
        assert args[0] == expected_key
        assert kwargs == {"ex": 15}

    def test_heartbeat_loop_does_nothing_when_not_alive(self, runner, mock_redis_client):
        runner._alive = False

        runner._heartbeat_loop()

        mock_redis_client.set.assert_not_called()

    def test_heartbeat_loop_multiple_heartbeats(self, runner, mock_redis_client, deployment_id):
        runner._alive = True

        thread = Thread(target=runner._heartbeat_loop, daemon=True)
        thread.start()
        time.sleep(2.5)

        runner._alive = False
        thread.join(timeout=2)

        assert mock_redis_client.set.call_count >= 2
        expected_key = f"{REDIS_STRATEGY_DEPLOYMENT_HEARTBEAT_KEY_PREFIX}{deployment_id}"
        for call_args in mock_redis_client.set.call_args_list:
            args, kwargs = call_args
            assert args[0] == expected_key
            assert kwargs == {"ex": 15}

    def test_heartbeat_loop_sets_alive_false_on_exception(self, runner, mock_redis_client):
        runner._alive = True
        mock_redis_client.set.side_effect = RuntimeError("redis down")

        with pytest.raises(RuntimeError):
            runner._heartbeat_loop()

        assert runner._alive is False


class TestSetup:

    @staticmethod
    def _make_mock_deployment(deployment_id, status=StrategyDeploymentStatus.STOPPED):
        mock = MagicMock()
        mock.deployment_id = deployment_id
        mock.status = status
        return mock

    @staticmethod
    def _make_mock_strategy_version():
        mock = MagicMock()
        mock.code = "class UserStrategy: pass"
        return mock

    def test_setup_loads_strategy_successfully(self, runner, deployment_id):
        mock_deployment = self._make_mock_deployment(deployment_id)
        mock_strategy_version = self._make_mock_strategy_version()

        mock_db_sess = MagicMock()
        mock_result = MagicMock()
        mock_result.first.return_value = (mock_deployment, mock_strategy_version)
        mock_db_sess.execute.return_value = mock_result
        mock_db_sess.commit = MagicMock()

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_db_sess
            mock_get_db.return_value = mock_context

            with patch(f"{MODULE_PATH}.StrategyLoader") as MockStrategyLoader:
                mock_loader = MagicMock()
                mock_strategy = MagicMock()
                mock_loader.load_strategy.return_value = mock_strategy
                MockStrategyLoader.return_value = mock_loader

                runner.setup()

                assert runner._strategy is mock_strategy
                mock_loader.load_strategy.assert_called_once_with(
                    mock_strategy_version.code
                )

    def test_setup_raises_when_deployment_not_found(self, runner):
        mock_db_sess = MagicMock()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        mock_db_sess.execute.return_value = mock_result

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_db_sess
            mock_get_db.return_value = mock_context

            with pytest.raises(ValueError, match="not found"):
                runner.setup()



    @pytest.mark.parametrize("status", [
        StrategyDeploymentStatus.RUNNING,
        StrategyDeploymentStatus.SUSPICIOUS,
        StrategyDeploymentStatus.CANCELLED,
        StrategyDeploymentStatus.STOP_REQUESTED,
    ])
    def test_setup_raises_when_deployment_not_stopped_or_pending(
        self, runner, deployment_id, status
    ):
        mock_deployment = MagicMock()
        mock_deployment.deployment_id = deployment_id
        mock_deployment.status = status

        mock_db_sess = MagicMock()
        mock_result = MagicMock()
        mock_strategy_version = MagicMock()
        mock_result.first.return_value = (mock_deployment, mock_strategy_version)
        mock_db_sess.execute.return_value = mock_result

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_db_sess
            mock_get_db.return_value = mock_context

            with pytest.raises(ValueError, match="not stopped"):
                runner.setup()

    def test_setup_succeeds_when_pending(self, runner, deployment_id):
        mock_deployment = self._make_mock_deployment(
            deployment_id, StrategyDeploymentStatus.PENDING
        )
        mock_strategy_version = self._make_mock_strategy_version()

        mock_db_sess = MagicMock()
        mock_result = MagicMock()
        mock_result.first.return_value = (mock_deployment, mock_strategy_version)
        mock_db_sess.execute.return_value = mock_result

        with patch(f"{MODULE_PATH}.get_db_sess_sync") as mock_get_db:
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_db_sess
            mock_get_db.return_value = mock_context

            with patch(f"{MODULE_PATH}.StrategyLoader") as MockStrategyLoader:
                mock_loader = MagicMock()
                mock_loader.load_strategy.return_value = MagicMock()
                MockStrategyLoader.return_value = mock_loader

                runner.setup()

                assert runner._strategy is not None


class TestRun:

    def test_run_publishes_running_and_stopped_events(
        self, runner, deployment_id, mock_event_publisher, mock_redis_client
    ):
        with patch.object(runner, "setup") as mock_setup:
            runner._strategy = MagicMock()

            runner.run()

        assert mock_event_publisher.publish.call_count == 2

        first_event = mock_event_publisher.publish.call_args_list[0][0][0]
        assert first_event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert first_event.deployment_id == deployment_id
        assert first_event.status == StrategyDeploymentStatus.RUNNING

        second_event = mock_event_publisher.publish.call_args_list[1][0][0]
        assert second_event.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert second_event.deployment_id == deployment_id
        assert second_event.status == StrategyDeploymentStatus.STOPPED

    def test_run_connects_ohlc_and_creates_oms_session(
        self, runner, mock_ohlc_feed_client, mock_oms_client
    ):
        with patch.object(runner, "setup"):
            runner._strategy = MagicMock()

            runner.run()

        mock_ohlc_feed_client.connect.assert_called_once()
        mock_oms_client.create_session.assert_called_once_with(runner._deployment_id)

    def test_run_calls_strategy_lifecycle(self, runner):
        mock_candle_1 = MagicMock()
        mock_candle_2 = MagicMock()

        ohlc_feed = MagicMock()
        ohlc_feed.candles.return_value = iter([mock_candle_1, mock_candle_2])
        runner._ohlc_feed_client = ohlc_feed

        with patch.object(runner, "setup"):
            mock_strategy = MagicMock()
            runner._strategy = mock_strategy

            runner.run()

        mock_strategy.startup.assert_called_once()
        assert mock_strategy.on_candle.call_args_list == [
            ((mock_candle_1,),),
            ((mock_candle_2,),),
        ]
        mock_strategy.shutdown.assert_called_once()

    def test_run_cleans_up_resources(self, runner, mock_ohlc_feed_client, mock_oms_client):
        with patch.object(runner, "setup"):
            runner._strategy = MagicMock()

            runner.run()

        mock_ohlc_feed_client.close.assert_called_once()
        mock_oms_client.disconnect.assert_called_once()
        assert runner._alive is False

    def test_run_breaks_candle_loop_when_alive_false(self, runner, mock_event_publisher):
        mock_candle_1 = MagicMock()
        mock_candle_2 = MagicMock()

        def candles_with_interrupt():
            yield mock_candle_1
            runner._alive = False
            yield mock_candle_2

        ohlc_feed = MagicMock()
        ohlc_feed.candles.return_value = candles_with_interrupt()
        runner._ohlc_feed_client = ohlc_feed

        with patch.object(runner, "setup"):
            mock_strategy = MagicMock()
            runner._strategy = mock_strategy

            runner.run()

        mock_strategy.on_candle.assert_called_once_with(mock_candle_1)
        assert mock_event_publisher.publish.call_count == 2

    def test_run_handles_keyboard_interrupt_gracefully(
        self,
        runner,
        mock_event_publisher,
        mock_oms_client,
    ):
        mock_candle = MagicMock()

        def candles_with_kb():
            yield mock_candle
            raise KeyboardInterrupt()

        ohlc_feed = MagicMock()
        ohlc_feed.candles.return_value = candles_with_kb()
        runner._ohlc_feed_client = ohlc_feed

        with patch.object(runner, "setup"):
            mock_strategy = MagicMock()
            runner._strategy = mock_strategy

            runner.run()

        assert mock_event_publisher.publish.call_count == 2
        second_event = mock_event_publisher.publish.call_args_list[1][0][0]
        assert second_event.status == StrategyDeploymentStatus.STOPPED

        mock_strategy.shutdown.assert_called_once()
        ohlc_feed.close.assert_called_once()
        mock_oms_client.disconnect.assert_called_once()
