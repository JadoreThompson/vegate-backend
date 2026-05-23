import pytest
import pytest_asyncio
from faker import Faker

from core.db import Base
from core.db.client import DB_ENGINE_SYNC


@pytest.fixture(scope="session", autouse=True)
def setup():
    from sqlalchemy import text

    with DB_ENGINE_SYNC.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        Base.metadata.create_all(bind=conn)

    yield


@pytest.fixture()
def faker():
    return Faker()
