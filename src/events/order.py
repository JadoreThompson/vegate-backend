from typing import Literal
from uuid import UUID

from events.base import BaseEvent, OrderEventType
from models import Order


class OrderPlaced(BaseEvent):
    """Event for when an order is placed."""

    type: Literal[OrderEventType.ORDER_PLACED] = OrderEventType.ORDER_PLACED
    deployment_id: UUID
    order: Order


class OrderCancelled(BaseEvent):
    """Event for when an order is cancelled."""

    type: Literal[OrderEventType.ORDER_CANCELLED] = OrderEventType.ORDER_CANCELLED
    deployment_id: UUID
    order_id: str
    success: bool


class OrderModified(BaseEvent):
    """Event for when an order is modified."""

    type: Literal[OrderEventType.ORDER_MODIFIED] = OrderEventType.ORDER_MODIFIED
    deployment_id: UUID
    order: Order
    success: bool
