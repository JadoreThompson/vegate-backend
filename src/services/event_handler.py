import json
import logging
from datetime import UTC, datetime

from redis.asyncio import Redis

from config import REDIS_ORDER_EVENTS_KEY, REDIS_SNAPSHOT_EVENTS_KEY
from enums import SnapshotType
from events.order import (
    OrderEventType,
    OrderPlacedEvent,
    OrderCancelledEvent,
    OrderModifiedEvent,
)
from events.snapshot import SnapshotCreated, SnapshotEventType
from infra.db import get_db_sess_sync
from infra.db.models import Orders, AccountSnapshots
from infra.db.models.strategy_deployments import StrategyDeployments
from infra.redis import REDIS_CLIENT


class OrderEventHandler:
    """Handles order events and performs database operations."""

    def __init__(self, redis_client: Redis = REDIS_CLIENT):
        """Initialize the order event handler.

        Args:
            redis_client: Redis client for subscribing to events
        """
        self._redis_client = redis_client
        self._logger = logging.getLogger(self.__class__.__name__)

    async def listen(self) -> None:
        """Listen for order events on Redis pub/sub and handle them."""
        try:
            async with self._redis_client.pubsub() as ps:
                await ps.subscribe(REDIS_ORDER_EVENTS_KEY)
                self._logger.info(
                    f"Subscribed to order events channel: {REDIS_ORDER_EVENTS_KEY}"
                )
                await ps.subscribe(REDIS_SNAPSHOT_EVENTS_KEY)
                self._logger.info(
                    f"Subscribed to snapshot events channel: {REDIS_SNAPSHOT_EVENTS_KEY}"
                )

                async for message in ps.listen():
                    if message["type"] == "message":
                        try:
                            event_data = json.loads(message["data"])
                            await self._handle_event(event_data)
                        except (json.JSONDecodeError, ValueError) as e:
                            self._logger.error(f"Failed to parse order event: {e}")
                            continue
                        except Exception as e:
                            self._logger.exception(f"Error handling order event: {e}")
                            continue

        except Exception as e:
            self._logger.exception(f"Error in order event listener: {e}")
            raise

    async def _handle_event(self, event_data: dict) -> None:
        """Handle a single order event.

        Args:
            event_data: Event data dictionary
        """
        event_type = event_data.get("type")

        if event_type == OrderEventType.ORDER_PLACED:
            await self._handle_order_placed(event_data)
        elif event_type == OrderEventType.ORDER_CANCELLED:
            await self._handle_order_cancelled(event_data)
        elif event_type == OrderEventType.ORDER_MODIFIED:
            await self._handle_order_modified(event_data)
        elif event_type == SnapshotEventType.SNAPSHOT_CREATED:
            await self._handle_snapshot_created(event_data)
        else:
            self._logger.warning(f"Unknown order event type: {event_type}")

    async def _handle_order_placed(self, event_data: dict) -> None:
        """Handle order placed event.

        Args:
            event_data: Event data containing order details
        """
        try:
            event = OrderPlacedEvent(**event_data)
            deployment_id = event.deployment_id
            order = event.order

            with get_db_sess_sync() as db_sess:
                # Create new order record
                db_order = Orders(
                    # order_id=(
                    #     UUID(order.order_id)
                    #     if isinstance(order.order_id, str)
                    #     else order.order_id
                    # ),
                    symbol=order.symbol,
                    side=order.side.value,
                    order_type=order.order_type.value,
                    quantity=order.quantity,
                    filled_quantity=order.executed_quantity,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    avg_fill_price=order.filled_avg_price,
                    status=order.status.value,
                    submitted_at=order.submitted_at,
                    filled_at=order.executed_at,
                    broker_order_id=order.order_id,
                    deployment_id=deployment_id,
                    details=order.details,
                )

                db_sess.add(db_order)
                db_sess.commit()

                self._logger.info(
                    f"Order placed: {db_order.order_id} for deployment {deployment_id}"
                )

        except Exception as e:
            self._logger.exception(f"Error handling order placed event: {e}")
            raise

    async def _handle_order_cancelled(self, event_data: dict) -> None:
        """Handle order cancelled event.

        Args:
            event_data: Event data containing order cancellation details
        """
        try:
            event = OrderCancelledEvent(**event_data)
            deployment_id = event.deployment_id
            order_id = event.order_id
            success = event.success

            if not success:
                self._logger.warning(
                    f"Order cancellation failed for order {order_id} in deployment {deployment_id}"
                )
                return

            with get_db_sess_sync() as db_sess:
                # Find and update order status
                db_order = (
                    db_sess.query(Orders)
                    .filter(
                        Orders.broker_order_id == order_id,
                        Orders.deployment_id == deployment_id,
                    )
                    .first()
                )

                if db_order:
                    db_order.status = "cancelled"
                    db_sess.commit()
                    self._logger.info(
                        f"Order cancelled: {order_id} for deployment {deployment_id}"
                    )
                else:
                    self._logger.warning(
                        f"Order not found for cancellation: {order_id} in deployment {deployment_id}"
                    )

        except Exception as e:
            self._logger.exception(f"Error handling order cancelled event: {e}")
            raise

    async def _handle_order_modified(self, event_data: dict) -> None:
        """Handle order modified event.

        Args:
            event_data: Event data containing modified order details
        """
        try:
            event = OrderModifiedEvent(**event_data)
            deployment_id = event.deployment_id
            order = event.order
            success = event.success

            if not success:
                self._logger.warning(
                    f"Order modification failed for order {order.order_id} in deployment {deployment_id}"
                )
                return

            with get_db_sess_sync() as db_sess:
                # Find and update order
                db_order = (
                    db_sess.query(Orders)
                    .filter(
                        Orders.broker_order_id == order.order_id,
                        Orders.deployment_id == deployment_id,
                    )
                    .first()
                )

                if db_order:
                    # Update order fields
                    db_order.quantity = order.quantity
                    db_order.limit_price = order.limit_price
                    db_order.stop_price = order.stop_price
                    db_order.status = order.status.value
                    db_sess.commit()
                    self._logger.info(
                        f"Order modified: {order.order_id} for deployment {deployment_id}"
                    )
                else:
                    self._logger.warning(
                        f"Order not found for modification: {order.order_id} in deployment {deployment_id}"
                    )

        except Exception as e:
            self._logger.exception(f"Error handling order modified event: {e}")
            raise

    async def _handle_snapshot_created(self, event_data: dict) -> None:
        """Handle snapshot created event.

        Args:
            event_data: Event data containing snapshot details
        """
        try:
            event = SnapshotCreated(**event_data)
            deployment_id = event.deployment_id
            snapshot_type = event.snapshot_type
            value = event.value

            with get_db_sess_sync() as db_sess:
                # Create new snapshot record
                db_snapshot = AccountSnapshots(
                    deployment_id=deployment_id,
                    timestamp=datetime.fromtimestamp(event.timestamp, UTC),
                    snapshot_type=snapshot_type,
                    value=value,
                )

                db_deployment = db_sess.get(StrategyDeployments, deployment_id)
                if (
                    db_deployment.starting_balance is None
                    and snapshot_type == SnapshotType.BALANCE
                ):
                    db_deployment.starting_balance = value

                db_sess.add(db_deployment)
                db_sess.add(db_snapshot)
                db_sess.commit()

                self._logger.info(
                    f"Snapshot created: {snapshot_type.value} = {value} for deployment {deployment_id}"
                )

        except Exception as e:
            self._logger.exception(f"Error handling snapshot created event: {e}")
            raise
