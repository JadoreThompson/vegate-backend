import asyncio
import logging
from typing import Type
from uuid import UUID

from sqlalchemy import case, select, update

from enums import EventStatus
from events.base import BaseEvent
from events.deployment import (
    DeploymentEventDeserialiser,
    DeploymentEventType,
)
from events.deserialiser import EventDeserialiser
from infra.db.model.event_outbox import EventOutbox
from infra.db.utils import get_db_session
from service.event.publisher import EventPublisher


class OutboxService:
    """
    Periodically publishes events pending within the outbox.
    """

    _DESERIALISERS: dict[Type, EventDeserialiser] = {}

    def __init__(
        self,
        interval: int,
        batch_size: int,
        event_publisher: EventPublisher,
    ):
        self.interval = interval
        self.batch_size = batch_size
        self._event_publisher = event_publisher

        self._logger = logging.getLogger(self.__class__.__name__)

    async def run(self):

        self._logger.info(
            "Starting outbox service (interval=%ss, batch_size=%s)",
            self.interval,
            self.batch_size,
        )

        while True:

            try:
                await asyncio.sleep(self.interval)

                events = await self._fetch_events()

                if not events:
                    self._logger.debug("No pending outbox events found")
                    continue

                self._logger.info("Processing %s outbox events", len(events))

                results = await asyncio.gather(
                    *[self._emit_event(record.id, record.payload) for record in events],
                    return_exceptions=True,
                )

                updates: list[tuple[UUID, EventStatus]] = []

                success_count = 0
                failed_count = 0

                for result in results:
                    if isinstance(result, Exception):
                        failed_count += 1

                        self._logger.exception(
                            "Unhandled exception while processing outbox batch",
                            exc_info=result,
                        )

                        continue

                    event_id, success = result

                    status = EventStatus.COMPLETED if success else EventStatus.FAILED

                    updates.append((event_id, status))

                    if success:
                        success_count += 1
                    else:
                        failed_count += 1

                if updates:
                    await self._update_events(updates)

                self._logger.info(
                    (
                        "Completed outbox batch "
                        "(processed=%s, succeeded=%s, failed=%s)"
                    ),
                    len(updates),
                    success_count,
                    failed_count,
                )

            except Exception as e:
                self._logger.exception(
                    "Unexpected error in outbox service loop", exc_info=e
                )

    async def _fetch_events(self):
        self._logger.debug(
            "Fetching pending outbox events (batch_size=%s)", self.batch_size
        )

        async with get_db_session() as db_sess:
            res = await db_sess.execute(
                select(EventOutbox)
                .where(
                    EventOutbox.status.in_(
                        [
                            EventStatus.PENDING,
                            EventStatus.FAILED,
                        ]
                    )
                )
                .order_by(EventOutbox.created_at.asc())
                .limit(self.batch_size)
            )

            events = res.scalars().all()

            self._logger.debug("Fetched %s outbox events", len(events))

            return events

    async def _emit_event(self, outbox_id: UUID, raw_event: dict) -> tuple[UUID, bool]:
        try:
            event = self._parse_event(raw_event)

            self._logger.debug(
                "Publishing event " "(outbox_id=%s, event_id=%s, type=%s)",
                outbox_id,
                event.id,
                event.type,
            )

            await self._event_publisher.publish(event)

            self._logger.info(
                "Successfully published event " "(outbox_id=%s, event_id=%s, type=%s)",
                outbox_id,
                event.id,
                event.type,
            )

            return outbox_id, True

        except Exception:

            self._logger.exception(
                "Failed to publish outbox event " "(outbox_id=%s, raw_type=%s)",
                outbox_id,
                raw_event.get("type"),
            )

            return outbox_id, False

    async def _update_events(
        self,
        event_id_status: list[tuple[UUID, EventStatus]],
    ):
        """
        Bulk update statuses in a single query.
        """

        if not event_id_status:
            return

        self._logger.debug("Updating %s outbox event statuses", len(event_id_status))

        ids = [event_id for event_id, _ in event_id_status]
        status_map = {event_id: status for event_id, status in event_id_status}

        stmt = (
            update(EventOutbox)
            .where(EventOutbox.id.in_(ids))
            .values(
                status=case(
                    status_map,
                    value=EventOutbox.id,
                )
            )
        )

        async with get_db_session() as db_sess:

            await db_sess.execute(stmt)
            await db_sess.commit()

        completed = sum(
            1 for _, status in event_id_status if status == EventStatus.COMPLETED
        )

        failed = sum(1 for _, status in event_id_status if status == EventStatus.FAILED)

        self._logger.info(
            ("Updated outbox statuses " "(completed=%s, failed=%s)"), completed, failed
        )

    def _parse_event(self, raw_event: dict) -> BaseEvent:
        cls = self.__class__

        event_type = raw_event["type"]

        self._logger.debug(
            "Parsing event type '%s'",
            event_type,
        )

        if event_type.startswith("deployment"):
            if DeploymentEventType not in cls._DESERIALISERS:
                self._logger.info("Initialising deployment event deserialiser")
                cls._DESERIALISERS[DeploymentEventType] = DeploymentEventDeserialiser()
                
            deserialiser = cls._DESERIALISERS[DeploymentEventType]

        else:
            raise ValueError(f"Unsupported event type '{event_type}'")

        return deserialiser.deserialise(raw_event)
