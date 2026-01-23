from typing import Literal
from uuid import UUID

from models import Order
from events.base import BaseEvent, EventType


class OrderPlaced(BaseEvent):
    """Event for when an order is placed."""

    type: Literal[EventType.ORDER_PLACED] = EventType.ORDER_PLACED
    strategy_id: UUID
    order: Order


class OrderCancelled(BaseEvent):
    """Event for when an order is cancelled."""

    type: Literal[EventType.ORDER_CANCELLED] = EventType.ORDER_CANCELLED
    strategy_id: UUID
    order_id: str
    success: bool


class OrderModified(BaseEvent):
    """Event for when an order is modified."""

    type: Literal[EventType.ORDER_MODIFIED] = EventType.ORDER_MODIFIED
    strategy_id: UUID
    order: Order
    success: bool
