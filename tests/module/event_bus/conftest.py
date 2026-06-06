from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import delete

from core.db import get_db_sess_sync
from core.kafka import AsyncKafkaProducer
from module.event_bus.model import EventOutbox


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield
    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(EventOutbox))
        db_sess.commit()


@pytest.fixture
def mock_kafka_producer():
    producer = MagicMock(spec=AsyncKafkaProducer)
    producer.start = AsyncMock()
    producer.send_and_wait = AsyncMock()
    producer.stop = AsyncMock()
    return producer
