import json
from uuid import uuid4

import pytest

from module.backtest.enums import BacktestStatus
from module.backtest.event import (
    BacktestCancelledEvent,
    BacktestEventType,
    BacktestRequestedEvent,
    BacktestStatusChangedEvent,
    BacktestStopRequestedEvent,
)
from module.backtest.event.deserialiser import BacktestEventDeserialiser
from module.backtest.schema import BacktestMetricsSchema


@pytest.fixture
def deserialiser():
    return BacktestEventDeserialiser()


class TestBacktestEventDeserialiser:

    def test_deserialise_status_changed(self, deserialiser):
        backtest_id = uuid4()
        event = BacktestStatusChangedEvent(
            backtest_id=backtest_id,
            status=BacktestStatus.IN_PROGRESS,
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == BacktestEventType.STATUS_CHANGED
        assert restored.backtest_id == backtest_id
        assert restored.status == BacktestStatus.IN_PROGRESS

    def test_deserialise_requested(self, deserialiser):
        backtest_id = uuid4()
        event = BacktestRequestedEvent(backtest_id=backtest_id)

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == BacktestEventType.REQUESTED
        assert restored.backtest_id == backtest_id

    def test_deserialise_stop_requested(self, deserialiser):
        backtest_id = uuid4()
        event = BacktestStopRequestedEvent(backtest_id=backtest_id)

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == BacktestEventType.STOP_REQUESTED
        assert restored.backtest_id == backtest_id

    def test_deserialise_cancelled(self, deserialiser):
        backtest_id = uuid4()
        reason = "User requested cancellation"
        event = BacktestCancelledEvent(
            backtest_id=backtest_id, reason=reason
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == BacktestEventType.CANCELLED
        assert restored.backtest_id == backtest_id
        assert restored.reason == reason

    def test_deserialise_json_from_string(self, deserialiser):
        backtest_id = uuid4()
        event = BacktestStatusChangedEvent(
            backtest_id=backtest_id,
            status=BacktestStatus.FAILED,
        )

        payload = event.model_dump_json()
        restored = deserialiser.deserialise_json(payload)

        assert restored.type == BacktestEventType.STATUS_CHANGED
        assert restored.backtest_id == backtest_id
        assert restored.status == BacktestStatus.FAILED

    def test_deserialise_json_from_bytes(self, deserialiser):
        backtest_id = uuid4()
        event = BacktestStatusChangedEvent(
            backtest_id=backtest_id,
            status=BacktestStatus.SUSPICIOUS,
        )

        payload = event.model_dump_json().encode()
        restored = deserialiser.deserialise_json(payload)

        assert restored.type == BacktestEventType.STATUS_CHANGED
        assert restored.backtest_id == backtest_id
        assert restored.status == BacktestStatus.SUSPICIOUS

    def test_deserialise_unknown_type_raises(self, deserialiser):
        data = {
            "id": str(uuid4()),
            "type": "backtest.unknown",
            "backtest_id": str(uuid4()),
            "timestamp": 1234567890,
        }

        with pytest.raises(ValueError, match="Unknown event type"):
            deserialiser.deserialise(data)

    def test_deserialise_missing_type_raises(self, deserialiser):
        data = {
            "id": str(uuid4()),
            "backtest_id": str(uuid4()),
            "timestamp": 1234567890,
        }

        with pytest.raises(ValueError, match="Missing event type"):
            deserialiser.deserialise(data)

    def test_round_trip_via_json(self, deserialiser):
        backtest_id = uuid4()
        original = BacktestStatusChangedEvent(
            backtest_id=backtest_id,
            status=BacktestStatus.PENDING,
        )

        payload = original.model_dump_json()
        restored = deserialiser.deserialise_json(payload)

        assert restored.id == original.id
        assert restored.backtest_id == original.backtest_id
        assert restored.status == original.status
        assert restored.type == original.type
        assert restored.timestamp == original.timestamp
