from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from config import STRATEGY_DEPLOYMENT_EVENTS_KEY
from events.deployment import DeploymentEventT, DeploymentEventDeserialiser, DeploymentEventType
from infra.db.model.deployment_event import DeploymentEvent
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.utils import get_db_session
from infra.kafka.client import AsyncKafkaConsumer


class EventConsumerService:
    """Consumes strategy deployment events and pushes them to the DB"""

    def __init__(self, deserialiser: DeploymentEventDeserialiser):
        self._deserialiser = deserialiser
        self._kafka_consumer: AsyncKafkaConsumer | None = None

    async def stop(self):
        if self._kafka_consumer:
            await self._kafka_consumer.stop()

    async def run(self):
        self._kafka_consumer = AsyncKafkaConsumer(
            STRATEGY_DEPLOYMENT_EVENTS_KEY,
            group_id="event_consumer__service_group",
            enable_auto_commit=False,
        )

        try:
            await self._kafka_consumer.start()
            async for record in self._kafka_consumer:
                await self._persist(self._deserialiser.deserialise_json(record.value))
                await self._kafka_consumer.commit()
        finally:
            await self._kafka_consumer.stop()

    async def _persist(self, event: DeploymentEventT) -> None:
        async with get_db_session() as session:
            # stmt = (
            #     insert(DeploymentEvent)
            #     .values(
            #         id=event.id,
            #         deployment_id=event.deployment_id,
            #         event_type=event.type,
            #         payload=event.model_dump(mode="json"),
            #         timestamp=event.timestamp,
            #     )
            #     .on_conflict_do_nothing(index_elements=["id"])
            # )
            await session.execute(
                insert(DeploymentEvent)
                .values(
                    id=event.id,
                    deployment_id=event.deployment_id,
                    event_type=event.type,
                    payload=event.model_dump(mode="json"),
                    timestamp=event.timestamp,
                )
                .on_conflict_do_nothing(index_elements=["id"])
            )

            if event.type == DeploymentEventType.DEPLOYMENT_STATUS:
                await session.execute(
                    update(StrategyDeployments)
                    .where(StrategyDeployments.deployment_id == event.deployment_id)
                    .values(status=event.status)
                )

            await session.commit()
