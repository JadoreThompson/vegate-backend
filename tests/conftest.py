import pytest
import pytest_asyncio
from faker import Faker

from infra.db.client import DB_ENGINE_SYNC
from infra.db.model.base import Base


@pytest.fixture(scope="session", autouse=True)
def setup():
    Base.metadata.drop_all(bind=DB_ENGINE_SYNC)
    Base.metadata.create_all(bind=DB_ENGINE_SYNC)

    yield

    Base.metadata.drop_all(bind=DB_ENGINE_SYNC)


@pytest.fixture()
def faker():
    return Faker()
