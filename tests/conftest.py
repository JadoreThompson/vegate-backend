import importlib
import pkgutil
from unittest.mock import AsyncMock, MagicMock

import pytest
from faker import Faker

from core.db import Base
from core.db.client import DB_ENGINE_SYNC


def import_modules():
    import module as package

    for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        importlib.import_module(module_info.name)


@pytest.fixture(scope="session", autouse=True)
def setup():
    from sqlalchemy import text

    import_modules()

    with DB_ENGINE_SYNC.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        Base.metadata.create_all(bind=conn)

    yield


@pytest.fixture()
def faker():
    return Faker()


@pytest.fixture
def mock_redis_client():
    return AsyncMock()
