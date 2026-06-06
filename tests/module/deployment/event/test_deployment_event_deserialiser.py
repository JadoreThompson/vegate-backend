from uuid import uuid4

import pytest

from vegate.oms.enums import OrderSide, OrderStatus, OrderType
from vegate.oms.schema import Order, OrderRequest
from module.deployment.enums import StrategyDeploymentStatus
from module.deployment.event import (
    DeploymentCancelOrderSubmitted,
    DeploymentErrorEvent,
    DeploymentEventType,
    DeploymentModifyOrderSubmitted,
    DeploymentOrderAcknowledged,
    DeploymentOrderRejected,
    DeploymentOrderSubmitted,
    DeploymentRequestedEvent,
    DeploymentStatusChangedEvent,
    DeploymentStopRequestedEvent,
)
from module.deployment.event.deserialiser import DeploymentEventDeserialiser
from module.deployment.event.event import DeploymentCancelledEvent


@pytest.fixture
def deserialiser():
    return DeploymentEventDeserialiser()


class TestDeploymentEventDeserialiser:

    def test_deserialise_status_changed(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.RUNNING,
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert restored.deployment_id == deployment_id
        assert restored.status == StrategyDeploymentStatus.RUNNING

    def test_deserialise_error(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentErrorEvent(
            deployment_id=deployment_id,
            error_msg="Something went wrong",
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_ERROR
        assert restored.deployment_id == deployment_id
        assert restored.error_msg == "Something went wrong"

    def test_deserialise_stop_requested(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentStopRequestedEvent(deployment_id=deployment_id)

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_STOP_REQUESTED
        assert restored.deployment_id == deployment_id

    def test_deserialise_order_submitted(self, deserialiser):
        deployment_id = uuid4()
        order_request = OrderRequest(
            symbol="AAPL",
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=10,
        )
        event = DeploymentOrderSubmitted(
            deployment_id=deployment_id,
            order=order_request,
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_ORDER_SUBMITTED
        assert restored.deployment_id == deployment_id
        assert restored.order.symbol == "AAPL"
        assert restored.order.quantity == 10

    def test_deserialise_cancel_order_submitted(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentCancelOrderSubmitted(
            deployment_id=deployment_id,
            order_id=uuid4(),
            broker_order_id="broker-123",
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_CANCEL_ORDER_SUBMITTED
        assert restored.deployment_id == deployment_id
        assert restored.broker_order_id == "broker-123"

    def test_deserialise_modify_order_submitted(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentModifyOrderSubmitted(
            deployment_id=deployment_id,
            order_id=uuid4(),
            broker_order_id="broker-456",
            limit_price=150.0,
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_MODIFY_ORDER_SUBMITTED
        assert restored.deployment_id == deployment_id
        assert restored.limit_price == 150.0
        assert restored.stop_price is None

    def test_deserialise_order_rejected(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentOrderRejected(
            deployment_id=deployment_id,
            order_id=uuid4(),
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_ORDER_REJECTED
        assert restored.deployment_id == deployment_id

    def test_deserialise_order_acknowledged(self, deserialiser):
        deployment_id = uuid4()
        order = Order(
            id="order-1",
            symbol="AAPL",
            filled_quantity=10,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
        )
        event = DeploymentOrderAcknowledged(
            deployment_id=deployment_id,
            order=order,
            broker_order_id="broker-789",
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_ORDER_ACKNOWLEDGED
        assert restored.deployment_id == deployment_id
        assert restored.broker_order_id == "broker-789"
        assert restored.order.symbol == "AAPL"

    def test_deserialise_requested(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentRequestedEvent(deployment_id=deployment_id)

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_REQUESTED
        assert restored.deployment_id == deployment_id

    def test_deserialise_cancelled(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentCancelledEvent(
            deployment_id=deployment_id,
            reason="capacity_constraint",
        )

        data = event.model_dump(mode="json")
        restored = deserialiser.deserialise(data)

        assert restored.type == DeploymentEventType.DEPLOYMENT_CANCELLED
        assert restored.deployment_id == deployment_id
        assert restored.reason == "capacity_constraint"

    def test_deserialise_json_from_string(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.RUNNING,
        )

        payload = event.model_dump_json()
        restored = deserialiser.deserialise_json(payload)

        assert restored.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert restored.deployment_id == deployment_id
        assert restored.status == StrategyDeploymentStatus.RUNNING

    def test_deserialise_json_from_bytes(self, deserialiser):
        deployment_id = uuid4()
        event = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.STOPPED,
        )

        payload = event.model_dump_json().encode()
        restored = deserialiser.deserialise_json(payload)

        assert restored.type == DeploymentEventType.DEPLOYMENT_STATUS
        assert restored.deployment_id == deployment_id
        assert restored.status == StrategyDeploymentStatus.STOPPED

    def test_deserialise_unknown_type_raises(self, deserialiser):
        data = {
            "id": str(uuid4()),
            "type": "deployment.unknown",
            "deployment_id": str(uuid4()),
            "timestamp": 1234567890,
        }

        with pytest.raises(ValueError, match="Unknown event type"):
            deserialiser.deserialise(data)

    def test_deserialise_missing_type_raises(self, deserialiser):
        data = {
            "id": str(uuid4()),
            "deployment_id": str(uuid4()),
            "timestamp": 1234567890,
        }

        with pytest.raises(ValueError, match="Missing event type"):
            deserialiser.deserialise(data)

    def test_round_trip_via_json(self, deserialiser):
        deployment_id = uuid4()
        original = DeploymentStatusChangedEvent(
            deployment_id=deployment_id,
            status=StrategyDeploymentStatus.PENDING,
        )

        payload = original.model_dump_json()
        restored = deserialiser.deserialise_json(payload)

        assert restored.id == original.id
        assert restored.deployment_id == original.deployment_id
        assert restored.status == original.status
        assert restored.type == original.type
        assert restored.timestamp == original.timestamp
