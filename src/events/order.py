from typing import Literal
from uuid import UUID

from models import Order
from events.base import BaseEvent, OrderEventType


class OrderPlaced(BaseEvent):
    """Event for when an order is placed."""

    type: Literal[OrderEventType.ORDER_PLACED] = OrderEventType.ORDER_PLACED
    strategy_id: UUID
    order: Order


class OrderCancelled(BaseEvent):
    """Event for when an order is cancelled."""

    type: Literal[OrderEventType.ORDER_CANCELLED] = OrderEventType.ORDER_CANCELLED
    strategy_id: UUID
    order_id: str
    success: bool


class OrderModified(BaseEvent):
    """Event for when an order is modified."""

    type: Literal[OrderEventType.ORDER_MODIFIED] = OrderEventType.ORDER_MODIFIED
    strategy_id: UUID
    order: Order
    success: bool
