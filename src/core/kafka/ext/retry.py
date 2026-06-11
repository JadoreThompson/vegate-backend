from __future__ import annotations

from typing import TYPE_CHECKING

from kafka.errors import NoBrokersAvailable
from module.exception.retry import Retry

if TYPE_CHECKING:
    from ..client import (
        KafkaConsumer,
        AsyncKafkaConsumer,
        KafkaProducer,
        AsyncKafkaProducer,
    )

kafka_retry = Retry(exceptions=[NoBrokersAvailable])


class KafkaRetryClient:

    def __init__(
        self,
        kafka_client: (
            KafkaConsumer | KafkaProducer | AsyncKafkaConsumer | AsyncKafkaProducer
        ),
    ):
        self.kafka_client = kafka_client

    def __aiter__(self):
        return self.kafka_client.__aiter__()

    async def __anext__(self):
        return await self.kafka_client.__anext__()

    def __getattribute__(self, name):
        if name == "kafka_client":
            return object.__getattribute__(self, "kafka_client")

        kafka_client = object.__getattribute__(self, "kafka_client")
        attr = getattr(kafka_client, name)

        if not callable(attr):
            return attr

        return kafka_retry(attr)
