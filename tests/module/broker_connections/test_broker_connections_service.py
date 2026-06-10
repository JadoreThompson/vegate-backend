import pytest
import pytest_asyncio
from datetime import datetime
from uuid import uuid4

from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import delete, insert

from core.db import get_db_sess_sync, get_db_session
from vegate.oms.enums import BrokerType
from module.broker_connections import BrokerConnectionsService
from module.broker_connections.exception import (
    BrokerAccountFetchException,
    BrokerConnectionNotFoundException,
)
from module.broker_connections.model import BrokerConnections
from module.broker_connections.schema import CreateBrokerConnectionRequest
from module.broker_connections.service import _BrokerAccount
from module.user.model import User


@pytest_asyncio.fixture(loop_scope="session")
async def broker_connections_service():
    return BrokerConnectionsService()


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(BrokerConnections))
        db_sess.commit()


@pytest_asyncio.fixture(loop_scope="session")
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


async def create_user(username: str):
    async with get_db_session() as db_sess:
        user = await db_sess.scalar(
            insert(User)
            .values(
                username=username,
                email=f"{username}@email.com",
                password="password",
                email_verified_at=datetime(year=2024, month=1, day=1),
            )
            .returning(User)
        )
        await db_sess.commit()

    return user


class TestCreateBrokerConnection:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_alpaca_connection_success(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = uuid4()
            mock_db_sess.add = MagicMock()

            with patch.object(
                broker_connections_service, "_fetch_alpaca_account_id"
            ) as mock_fetch:
                mock_fetch.return_value = _BrokerAccount(id="acc-123", number="ACC-001")

                request = CreateBrokerConnectionRequest(
                    broker=BrokerType.ALPACA,
                    api_key="test-key",
                    secret_key="test-secret",
                )

                result = await broker_connections_service.create_broker_connection(
                    request, uuid4(), mock_db_sess
                )

                assert isinstance(result, BrokerConnections)
                assert result.broker == BrokerType.ALPACA
                assert result.broker_account_id == "acc-123"
                assert result.broker_account_number == "ACC-001"

        @pytest.mark.asyncio(loop_scope="session")
        async def test_fetch_alpaca_account_invalid_keys_raises(
            self, broker_connections_service
        ):
            mock_get = AsyncMock()
            mock_http_sess = AsyncMock()
            mock_http_sess.get = mock_get
            broker_connections_service.get_http_session = MagicMock(
                return_value=mock_http_sess
            )

            mock_response = MagicMock()
            mock_response.ok = False
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            with pytest.raises(BrokerAccountFetchException):
                await broker_connections_service._fetch_alpaca_account_id(
                    "invalid-key", "invalid-secret"
                )

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_broker_connection_stores_in_db(
            self, broker_connections_service, db_sess
        ):
            with patch.object(
                broker_connections_service, "_fetch_alpaca_account_id"
            ) as mock_fetch:
                mock_fetch.return_value = _BrokerAccount(
                    id="acc-integration-123", number="INT-001"
                )

                user = await create_user("create-user")

                request = CreateBrokerConnectionRequest(
                    broker=BrokerType.ALPACA,
                    api_key="test-api-key",
                    secret_key="test-secret-key",
                )

                result = await broker_connections_service.create_broker_connection(
                    request, user.user_id, db_sess
                )

                await db_sess.commit()

                async with get_db_session() as new_db_sess:
                    conn = await new_db_sess.get(
                        BrokerConnections, result.connection_id
                    )

                assert conn is not None
                assert conn.user_id == user.user_id
                assert conn.broker == BrokerType.ALPACA
                assert conn.api_key == "test-api-key"
                assert conn.broker_account_id == "acc-integration-123"


class TestGetBrokerConnections:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_connections_returns_paginated_response(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()

            mock_conn = MagicMock()
            mock_conn.connection_id = uuid4()
            mock_conn.broker = BrokerType.ALPACA
            mock_conn.broker_account_id = "acc-123"
            mock_conn.broker_account_number = "ACC-001"

            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = [mock_conn]

            mock_db_sess.execute.return_value = mock_result

            result = await broker_connections_service.get_broker_connections(
                uuid4(), mock_db_sess, page=1, limit=10
            )

            assert result.page == 1
            assert result.size == 1
            assert len(result.data) == 1
            assert result.data[0].broker == BrokerType.ALPACA

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_connections_returns_user_connections(
            self, broker_connections_service, db_sess
        ):
            user_a = await create_user(username="user_a")
            user_b = await create_user(username="user_b")

            user_a_bconns = [
                BrokerConnections(
                    broker=BrokerType.ALPACA,
                    user_id=user_a.user_id,
                    api_key="<api-key>",
                    secret_key="<secret-key>",
                    broker_account_id=f"account-{i}",
                    broker_account_number=f"account-number-{i}",
                )
                for i in range(100)
            ]
            db_sess.add_all(user_a_bconns)

            user_b_bconns = [
                BrokerConnections(
                    broker=BrokerType.ALPACA,
                    user_id=user_b.user_id,
                    api_key="<api-key>",
                    secret_key="<secret-key>",
                    broker_account_id=f"account-{i}",
                    broker_account_number=f"account-number-{i}",
                )
                for i in range(100)
            ]
            db_sess.add_all(user_b_bconns)

            await db_sess.commit()

            async with get_db_session() as db_sess:
                result = await broker_connections_service.get_broker_connections(
                    user_a.user_id, db_sess, page=1, limit=10
                )

            assert len(result.data) == 10
            assert all(
                bc.id == user_a_bconns[i].connection_id
                for i, bc in enumerate(result.data)
            )

            async with get_db_session() as db_sess:
                result = await broker_connections_service.get_broker_connections(
                    user_a.user_id, db_sess, page=2, limit=10
                )

            assert len(result.data) == 10
            assert all(
                bc.id == user_a_bconns[i + 10].connection_id
                for i, bc in enumerate(result.data)
            )


class TestGetBrokerConnection:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_connection_returns_connection_when_found(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()

            mock_conn = MagicMock()
            mock_db_sess.scalar.return_value = mock_conn

            result = await broker_connections_service.get_broker_connection(
                uuid4(), uuid4(), mock_db_sess
            )

            assert result == mock_conn

        @pytest.mark.asyncio(loop_scope="session")
        async def test_get_connection_returns_none_when_not_found(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with pytest.raises(BrokerConnectionNotFoundException):
                result = await broker_connections_service.get_broker_connection(
                    uuid4(), uuid4(), mock_db_sess
                )


class TestDeleteBrokerConnection:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_connection_returns_true_when_deleted(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_db_sess.execute.return_value = mock_result

            result = await broker_connections_service.delete_broker_connection(
                uuid4(), uuid4(), mock_db_sess
            )

            assert result is True

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_connection_returns_false_when_not_found(
            self, broker_connections_service
        ):
            mock_db_sess = AsyncMock()

            mock_result = MagicMock()
            mock_result.rowcount = 0
            mock_db_sess.execute.return_value = mock_result

            result = await broker_connections_service.delete_broker_connection(
                uuid4(), uuid4(), mock_db_sess
            )

            assert result is False

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_connection_removes_from_db(
            self, broker_connections_service, db_sess
        ):
            user = await create_user(username="delete-user")
            broker_conn = BrokerConnections(
                broker=BrokerType.ALPACA,
                user_id=user.user_id,
                api_key="<api-key>",
                secret_key="<secret-key>",
                broker_account_id=f"account-{user.user_id}",
                broker_account_number=f"account-number-{user.user_id}",
            )
            db_sess.add(broker_conn)
            await db_sess.commit()

            async with get_db_session() as new_db_sess:
                conn = await broker_connections_service.get_broker_connection(
                    broker_conn.connection_id, user.user_id, new_db_sess
                )

            assert conn.connection_id == broker_conn.connection_id

            async with get_db_session() as new_db_sess:
                success = await broker_connections_service.delete_broker_connection(
                    broker_conn.connection_id, user.user_id, new_db_sess
                )
                await new_db_sess.commit()

            assert success

            async with get_db_session() as new_db_sess:
                broker_conn = await new_db_sess.get(
                    BrokerConnections, broker_conn.connection_id
                )

            assert broker_conn is None
