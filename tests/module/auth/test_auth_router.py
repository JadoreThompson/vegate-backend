import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import update


from core.db.session import get_db_sess_sync, get_db_session
from module.api.object_registry import ObjectRegistry
from module.auth import AuthService
from core.redis import REDIS_CLIENT
from module.user.model import User
from util import get_datetime


@pytest.fixture
def email_service():
    service = MagicMock()
    service.send_email = AsyncMock(return_value=None)
    return service


@pytest.fixture
def auth_service():
    from module.api.app import app

    object_registry: ObjectRegistry = app.state.object_registry
    service = object_registry.get(AuthService)
    return service


@pytest.fixture(scope="module", autouse=True)
def clear_tables():
    yield

    from sqlalchemy import delete
    from module.user.model import User

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(User))
        db_sess.commit()


class TestRegisterEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_success_returns_201(self, client, monkeypatch):
        payload = {
            "username": "test-user",
            "email": "test@email.com",
            "password": "PAssword1@@1",
        }

        res = await client.post("/api/v1/auth/register", json=payload)

        assert res.status_code == 201

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_invalid_password_returns_422(self, client):
        payload = {
            "username": "test-user",
            "email": "invalid-email",
            # missing password
        }

        res = await client.post("/api/v1/auth/register", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_invalid_password_returns_422(self, client):
        payloads = [
            {"username": "test-user", "email": "user@gmail.com", "password": "p"},
            {
                "username": "test-user",
                "email": "user@gmail.com",
                "password": "password10",
            },
            {
                "username": "test-user",
                "email": "user@gmail.com",
                "password": "password@@",
            },
        ]

        for payload in payloads:
            res = await client.post("/api/v1/auth/register", json=payload)
            assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_register_invalid_email_returns_422(self, client):
        payloads = [
            {"username": "test-user", "email": "user@", "password": "9:password9:"},
            {
                "username": "test-user",
                "email": "user@email-com",
                "password": "9:password9:",
            },
        ]

        for payload in payloads:
            res = await client.post("/api/v1/auth/register", json=payload)
            assert res.status_code == 422


class TestLoginEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_success_with_username_returns_200(self, client):
        register_payload = {
            "username": "logintest-user",
            "email": "logintest@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        async with get_db_session() as db_sess:
            await db_sess.execute(
                update(User)
                .where(User.username == register_payload["username"])
                .values(authenticated_at=get_datetime())
            )
            await db_sess.commit()

        login_payload = {
            "username": "logintest-user",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_success_with_email_returns_200(self, client):
        register_payload = {
            "username": "logintest2-user",
            "email": "logintest2@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        async with get_db_session() as db_sess:
            await db_sess.execute(
                update(User)
                .where(User.email == register_payload["email"])
                .values(authenticated_at=get_datetime())
            )
            await db_sess.commit()

        login_payload = {
            "email": "logintest2@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 200

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_success_with_email_returns_403(self, client):
        register_payload = {
            "username": "logintest-unauthenticated_user-1",
            "email": "logintest-unauthenticated_user-1@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        login_payload = {
            "email": "logintest-unauthenticated_user-1@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 403

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_missing_username_and_email_returns_422(self, client):
        login_payload = {
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_missing_password_returns_422(self, client):
        login_payload = {
            "username": "someuser",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_invalid_credentials_returns_422(self, client):
        register_payload = {
            "username": "logintest3-user",
            "email": "logintest3@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        login_payload = {
            "username": "logintest3-user",
            "password": "WrongPassword1@@",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_login_nonexistent_user_returns_422(self, client):
        login_payload = {
            "username": "nonexistent-user",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/login", json=login_payload)

        assert res.status_code == 422


class TestVerifyEmailRequestEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_verify_email_request_unauthenticated_returns_201(self, client):
        register_payload = {
            "username": "verifyreq-user",
            "email": "verifyreq@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        res = await client.post("/api/v1/auth/verify-email/request")

        assert res.status_code == 201


class TestVerifyEmailEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_verify_email_invalid_code_returns_400(self, client):
        register_payload = {
            "username": "verify-user",
            "email": "verify@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        verify_payload = {"code": "invalid"}
        res = await client.post("/api/v1/auth/verify-email", json=verify_payload)

        assert res.status_code == 400


class TestLogoutEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_logout_returns_200(self, client):
        register_payload = {
            "username": "logout-user",
            "email": "logout@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        res = await client.post("/api/v1/auth/logout")

        assert res.status_code == 200


class TestChangeUsernameRequestEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_request_returns_201(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changeuser-user",
            "email": "changeuser@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        res = await client.post(
            "/api/v1/auth/verify-email", json={"code": verification_code}
        )

        payload = {"username": "new-username"}
        res = await client.post("/api/v1/auth/change-username/request", json=payload)

        assert res.status_code == 201, res.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_request_duplicate_returns_400(
        self, client, auth_service
    ):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changeuser2-user",
            "email": "changeuser2@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        res = await client.post(
            "/api/v1/auth/verify-email", json={"code": verification_code}
        )

        payload = {"username": "changeuser2-user"}
        res = await client.post("/api/v1/auth/change-username/request", json=payload)

        assert res.status_code == 400

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changeuser-user",
            "email": "changeuser@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"username": "new-username"}
        res = await client.post("/api/v1/auth/change-username/request", json=payload)

        assert res.status_code == 401, res.json()


class TestChangeUsernameEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_invalid_code_returns_400(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changeuser3-user",
            "email": "changeuser3@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"code": "invalid"}
        res = await client.post("/api/v1/auth/change-username", json=payload)

        assert res.status_code == 400

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_valid_code_returns_200(self, client, auth_service):
        verification_code = "ABC123"
        change_username_code = "change-username-code"
        auth_service.gen_verification_code = MagicMock(
            side_effect=[verification_code, change_username_code]
        )

        register_payload = {
            "username": "changeuser4-user",
            "email": "changeuser4@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        res = await client.post(
            "/api/v1/auth/verify-email", json={"code": verification_code}
        )
        assert res.status_code == 200, res.json()

        await client.post(
            "/api/v1/auth/change-username/request",
            json={"username": "changeuser5-user"},
        )

        res = await client.post(
            "/api/v1/auth/change-username", json={"code": change_username_code}
        )

        assert res.status_code == 202, res.json()

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_username_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changeuser6-user",
            "email": "changeuser6@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"username": "new-username"}
        res = await client.post("/api/v1/auth/change-username", json=payload)

        assert res.status_code == 401, res.json()


class TestChangePasswordRequestEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_request_returns_201(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changepw-user",
            "email": "changepw@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"password": "NewP@ssword1@@1"}
        res = await client.post("/api/v1/auth/change-password/request", json=payload)

        assert res.status_code == 201

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_request_invalid_password_returns_422(
        self, client, auth_service
    ):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changepw2-user",
            "email": "changepw2@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"password": "weak"}
        res = await client.post("/api/v1/auth/change-password/request", json=payload)

        assert res.status_code == 422

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changepw3-user",
            "email": "changepw3@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"username": "new-username"}
        res = await client.post("/api/v1/auth/change-password", json=payload)

        assert res.status_code == 401, res.json()


class TestChangePasswordEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_invalid_code_returns_400(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changepw4-user",
            "email": "changepw4@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"code": "invalid"}
        res = await client.post("/api/v1/auth/change-password", json=payload)

        assert res.status_code == 400

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changepw5-user",
            "email": "changepw5@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"password": "new-username"}
        res = await client.post("/api/v1/auth/change-password", json=payload)

        assert res.status_code == 401, res.json()


class TestChangeEmailRequestEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_email_request_returns_202(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changeemail-user",
            "email": "changeemail@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"email": "newemail@email.com"}
        res = await client.post("/api/v1/auth/change-email/request", json=payload)

        assert res.status_code == 202

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_email_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changeemail2-user",
            "email": "changeemail2@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"emailsername": "new@email.com"}
        res = await client.post("/api/v1/auth/change-email", json=payload)

        assert res.status_code == 401, res.json()


class TestChangeEmailEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_email_invalid_code_returns_400(self, client, auth_service):
        verification_code = "ABC123"
        auth_service.gen_verification_code = MagicMock(return_value=verification_code)

        register_payload = {
            "username": "changeemail3-user",
            "email": "changeemail3@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        await client.post("/api/v1/auth/verify-email", json={"code": verification_code})

        payload = {"code": "invalid"}
        res = await client.post("/api/v1/auth/change-email", json=payload)

        assert res.status_code == 400

    @pytest.mark.asyncio(loop_scope="session")
    async def test_change_password_unauthenticated_returns_401(self, client):
        register_payload = {
            "username": "changeemail4-user",
            "email": "changeemail4@email.com",
            "password": "PAssword1@@1",
        }
        res = await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"email": "new-email@email.com"}
        res = await client.post("/api/v1/auth/change-email", json=payload)

        assert res.status_code == 401, res.json()


class TestResetPasswordRequestEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reset_password_request_returns_201(self, client):
        register_payload = {
            "username": "resetpw-user",
            "email": "resetpw@email.com",
            "password": "PAssword1@@1",
        }
        await client.post("/api/v1/auth/register", json=register_payload)

        payload = {"email": "resetpw@email.com"}
        res = await client.post("/api/v1/auth/reset-password/request", json=payload)

        assert res.status_code == 201

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reset_password_request_nonexistent_email_returns_201(self, client):
        payload = {"email": "nonexistent@email.com"}
        res = await client.post("/api/v1/auth/reset-password/request", json=payload)

        assert res.status_code == 201


class TestResetPasswordEndpoint:

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reset_password_invalid_code_returns_400(self, client):
        payload = {"code": "invalid", "password": "PAssword1@@1"}
        res = await client.patch("/api/v1/auth/reset-password", json=payload)

        assert res.status_code == 400

    @pytest.mark.asyncio(loop_scope="session")
    async def test_reset_password_invalid_password_returns_422(self, client):
        payload = {"code": "somedummycode", "password": "weak"}
        res = await client.patch("/api/v1/auth/reset-password", json=payload)

        assert res.status_code == 422
