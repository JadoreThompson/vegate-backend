from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import delete

from core.db import get_db_session, get_db_sess_sync
from module.user.model import User
from util import get_uuid


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(User))
        db_sess.commit()


@pytest_asyncio.fixture
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestGetUserById:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_by_id_returns_user_when_exists(self):
            mock_db_sess = AsyncMock()

            expected_user = MagicMock(spec=User)
            expected_user.user_id = uuid4()
            expected_user.username = "test-user"
            expected_user.email = "test@email.com"

            mock_db_sess.get.return_value = expected_user

            user = await mock_db_sess.get(User, expected_user.user_id)

            assert user is expected_user
            assert user.username == "test-user"

            mock_db_sess.get.assert_awaited_once_with(User, expected_user.user_id)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_by_id_returns_none_when_not_found(self):
            mock_db_sess = AsyncMock()

            user_id = uuid4()
            mock_db_sess.get.return_value = None

            user = await mock_db_sess.get(User, user_id)

            assert user is None

            mock_db_sess.get.assert_awaited_once_with(User, user_id)

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_by_id_returns_persisted_user(self, db_sess):
            user_id = get_uuid()
            user = User(
                user_id=user_id,
                username="persist-user",
                email="persist-user@email.com",
                password="hashed-password",
            )
            db_sess.add(user)
            await db_sess.commit()

            async with get_db_session() as new_db_sess:
                fetched = await new_db_sess.get(User, user_id)

            assert fetched is not None
            assert fetched.user_id == user_id
            assert fetched.username == "persist-user"
            assert fetched.email == "persist-user@email.com"

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_user_by_id_returns_none_for_nonexistent(self, db_sess):
            async with get_db_session() as new_db_sess:
                fetched = await new_db_sess.get(User, uuid4())

            assert fetched is None
