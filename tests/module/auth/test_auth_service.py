import json
import pytest
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio
from sqlalchemy import delete

from core.db import get_db_sess_sync, get_db_session
from core.redis import REDIS_CLIENT
from config import (
    REDIS_CHANGE_EMAIL_KEY_PREFIX,
    REDIS_CHANGE_PASSWORD_KEY_PREFIX,
    REDIS_CHANGE_USERNAME_KEY_PREFIX,
    REDIS_PASSWORD_RESET_EXPIRY_SECS,
    REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX,
    VERIFICATION_CODE_EXPIRY_SECS,
)
from module.auth import AuthService
from module.auth.exception import UserAlreadyExistsException, UserDoesNotExistException
from module.auth.schema import (
    ChangeEmailRequest,
    ChangePasswordRequest,
    ChangeUsernameRequest,
    LoginUserRequest,
    RegisterUserRequest,
    ResetPasswordRequest,
    ResetPasswordResponse,
    VerificationCode,
)
from module.user.model import User


@pytest.fixture
def redis_client():
    return AsyncMock()


@pytest.fixture
def auth_service(redis_client):
    return AuthService(email_service_cls=MagicMock, redis_client=redis_client)


@pytest.fixture
def email_service(auth_service):
    email_service = auth_service._email_service
    return email_service


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


class TestRegisterUser:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_with_existing_username_raises(self, auth_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute.side_effect = [
                MagicMock(),
                Exception("Execute should have been called only one time"),
            ]
            request = RegisterUserRequest(
                username="test-user",
                email="test-user@email.com",
                password="PAssword1@@1",
            )

            with pytest.raises(UserAlreadyExistsException):
                await auth_service.register_user(request, mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_with_existing_email_raises(self, auth_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.execute.side_effect = [
                MagicMock(),
                Exception("Execute should have been called only one time"),
            ]
            request = RegisterUserRequest(
                username="test-user",
                email="test-user@email.com",
                password="PAssword1@@1",
            )

            with pytest.raises(UserAlreadyExistsException):
                await auth_service.register_user(request, mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_sends_email_verification(
            self, auth_service, email_service
        ):
            email_service.send_email = AsyncMock()

            mock_db_sess = AsyncMock()
            mock_first = MagicMock()
            mock_first.first.return_value = None
            mock_db_sess.execute.side_effect = [mock_first, MagicMock()]

            request = RegisterUserRequest(
                username="test-user",
                email="test-user@email.com",
                password="PAssword1@@1",
            )

            await auth_service.register_user(request, mock_db_sess)

            assert email_service.send_email.call_count == 1
            assert email_service.send_email.await_args.args[1] == "Verify your email"

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_user_created_with_hashed_password(self, auth_service, db_sess):
            mock_send_verification_code = AsyncMock()
            mock_send_verification_code._send_verification_code.side_effect = (
                lambda *args, **kw: None
            )
            auth_service._send_verification_code = mock_send_verification_code

            request = RegisterUserRequest(
                username="test-user2",
                email="test-user2@email.com",
                password="PAssword1@@1",
            )
            user = await auth_service.register_user(request, db_sess)
            await db_sess.commit()

            async with get_db_session() as new_db_sess:
                user = await new_db_sess.get(User, user.user_id)

            assert user.password != request.password
            assert auth_service.verify_password(request.password, user.password)


class TestAuthenticateUser:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_without_username_or_email_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            request = LoginUserRequest(
                username=None, email=None, password="PAssword1@@1"
            )

            with pytest.raises(
                ValueError, match="Either username or email must be provided"
            ):
                await auth_service.authenticate_user(request, mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_with_unknown_user_raises(self, auth_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            request = LoginUserRequest(
                username="unknown-user", email=None, password="PAssword1@@1"
            )

            with pytest.raises(ValueError, match="Incorrect credentials"):
                await auth_service.authenticate_user(request, mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_with_invalid_password_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            mock_user = MagicMock()
            mock_user.password = "hashed-password"

            mock_db_sess.scalar.return_value = mock_user

            auth_service.verify_password = MagicMock(return_value=False)

            request = LoginUserRequest(
                username="test-user", email=None, password="wrong-password"
            )

            with pytest.raises(ValueError, match="Incorrect credentials"):
                await auth_service.authenticate_user(request, mock_db_sess)

            auth_service.verify_password.assert_called_once_with(
                "wrong-password", "hashed-password"
            )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_success(self, auth_service):
            mock_db_sess = AsyncMock()

            mock_user = MagicMock()
            mock_user.password = "hashed-password"

            mock_db_sess.scalar.return_value = mock_user

            auth_service.verify_password = MagicMock(return_value=True)

            request = LoginUserRequest(
                username="test-user", email=None, password="PAssword1@@1"
            )

            user = await auth_service.authenticate_user(request, mock_db_sess)

            assert user == mock_user

            auth_service.verify_password.assert_called_once_with(
                "PAssword1@@1", "hashed-password"
            )

    class TestIntegrationTest:
        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_with_username_success(
            self, auth_service, db_sess
        ):
            mock_send_verification_code = AsyncMock()
            auth_service._send_verification_code = mock_send_verification_code

            register_request = RegisterUserRequest(
                username="login-user",
                email="login-user@email.com",
                password="PAssword1@@1",
            )

            created_user = await auth_service.register_user(
                register_request,
                db_sess,
            )

            await db_sess.commit()

            login_request = LoginUserRequest(
                username="login-user",
                email=None,
                password="PAssword1@@1",
            )

            async with get_db_session() as new_db_sess:
                authenticated_user = await auth_service.authenticate_user(
                    login_request,
                    new_db_sess,
                )

            assert authenticated_user.user_id == created_user.user_id
            assert authenticated_user.username == "login-user"

        @pytest.mark.asyncio(loop_scope="session")
        async def test_authenticate_user_with_email_success(
            self, auth_service, db_sess
        ):
            mock_send_verification_code = AsyncMock()
            auth_service._send_verification_code = mock_send_verification_code

            register_request = RegisterUserRequest(
                username="email-login-user",
                email="email-login-user@email.com",
                password="PAssword1@@1",
            )

            created_user = await auth_service.register_user(
                register_request,
                db_sess,
            )

            await db_sess.commit()

            login_request = LoginUserRequest(
                username=None,
                email="email-login-user@email.com",
                password="PAssword1@@1",
            )

            async with get_db_session() as new_db_sess:
                authenticated_user = await auth_service.authenticate_user(
                    login_request,
                    new_db_sess,
                )

            assert authenticated_user.user_id == created_user.user_id
            assert authenticated_user.email == "email-login-user@email.com"


class TestVerifyEmail:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_with_invalid_code_raises(self, auth_service):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            auth_service._redis_client.get.return_value = b"ABC123"

            request = VerificationCode(code="WRONG1")

            with pytest.raises(
                ValueError, match="Invalid or expired verification code"
            ):
                await auth_service.verify_email(request, user_id, mock_db_sess)

            auth_service._redis_client.delete.assert_not_called()

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_with_missing_code_raises(self, auth_service):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            auth_service._redis_client.get.return_value = None

            request = VerificationCode(code="ABC123")

            with pytest.raises(
                ValueError, match="Invalid or expired verification code"
            ):
                await auth_service.verify_email(request, user_id, mock_db_sess)

            auth_service._redis_client.delete.assert_not_called()

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_success(self, auth_service):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            mock_user = MagicMock()
            mock_user.authenticated_at = None

            auth_service._redis_client.get.return_value = b"ABC123"
            mock_db_sess.scalar.return_value = mock_user

            request = VerificationCode(code="ABC123")

            user = await auth_service.verify_email(request, user_id, mock_db_sess)

            assert user == mock_user
            assert user.authenticated_at is not None

            auth_service._redis_client.delete.assert_awaited_once_with(
                f"{auth_service._email_verification_key_prefix}{user_id}"
            )

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_updates_authenticated_at(
            self, auth_service, db_sess
        ):
            mock_send_verification_code = AsyncMock()
            auth_service._send_verification_code = mock_send_verification_code

            request = RegisterUserRequest(
                username="verify-user",
                email="verify-user@email.com",
                password="PAssword1@@1",
            )

            user = await auth_service.register_user(request, db_sess)
            await db_sess.commit()

            redis_key = f"{auth_service._email_verification_key_prefix}{user.user_id}"

            auth_service._redis_client.get.return_value = b"ABC123"
            auth_service._redis_client.delete.return_value = None

            verification_request = VerificationCode(code="ABC123")

            async with get_db_session() as new_db_sess:
                updated_user = await auth_service.verify_email(
                    verification_request,
                    user.user_id,
                    new_db_sess,
                )

                await new_db_sess.commit()

            assert updated_user.authenticated_at is not None

            async with get_db_session() as new_db_sess:
                persisted_user = await new_db_sess.get(User, user.user_id)

            assert persisted_user.authenticated_at is not None

            auth_service._redis_client.delete.assert_awaited_once_with(redis_key)


class TestRequestEmailChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_email_change_with_missing_user_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            mock_db_sess.scalar.side_effect = [None]

            request = ChangeEmailRequest(email="new-email@email.com")

            with pytest.raises(UserDoesNotExistException):
                await auth_service.request_email_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_email_change_with_existing_email_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            mock_user = MagicMock()
            existing_user = MagicMock()

            mock_db_sess.scalar.side_effect = [
                mock_user,
                existing_user,
            ]

            request = ChangeEmailRequest(email="existing@email.com")

            with pytest.raises(
                UserAlreadyExistsException, match="Email already exists"
            ):
                await auth_service.request_email_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_email_change_sets_redis_and_sends_email(
            self, auth_service, email_service
        ):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            mock_user = MagicMock()
            mock_user.email = "current@email.com"

            mock_db_sess.scalar.side_effect = [mock_user, None]

            auth_service._redis_client.set = AsyncMock()

            email_service.send_email = AsyncMock()

            request = ChangeEmailRequest(email="new@email.com")

            await auth_service.request_email_change(
                request,
                user_id,
                mock_db_sess,
            )

            auth_service._redis_client.set.assert_awaited_once()

            redis_call_args = auth_service._redis_client.set.await_args

            assert (
                redis_call_args.args[0] == f"{REDIS_CHANGE_EMAIL_KEY_PREFIX}{user_id}"
            )

            payload = json.loads(redis_call_args.args[1])

            assert payload["email"] == "new@email.com"
            assert "code" in payload

            assert redis_call_args.kwargs["ex"] == VERIFICATION_CODE_EXPIRY_SECS

            email_service.send_email.assert_awaited_once()

            email_args = email_service.send_email.await_args.args

            assert email_args[0] == "current@email.com"
            assert email_args[1] == "Confirm Your Email Change"

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_email_change_stores_verification_payload(
            self, auth_service, db_sess, email_service
        ):
            email_service.send_email = AsyncMock()

            mock_send_verification_code = AsyncMock()
            auth_service._send_verification_code = mock_send_verification_code

            register_request = RegisterUserRequest(
                username="change-email-user",
                email="change-email-user@email.com",
                password="PAssword1@@1",
            )

            user = await auth_service.register_user(
                register_request,
                db_sess,
            )

            await db_sess.commit()

            auth_service._redis_client = REDIS_CLIENT

            change_request = ChangeEmailRequest(email="updated@email.com")
            async with get_db_session() as new_db_sess:
                await auth_service.request_email_change(
                    change_request, user.user_id, new_db_sess
                )

            payload = json.loads(
                await auth_service._redis_client.get(
                    f"{REDIS_CHANGE_EMAIL_KEY_PREFIX}{user.user_id}"
                )
            )

            assert payload["email"] == "updated@email.com"
            assert "code" in payload

            email_service.send_email.assert_awaited_once()

            email_args = email_service.send_email.await_args.args

            assert email_args[0] == "change-email-user@email.com"
            assert email_args[1] == "Confirm Your Email Change"


class TestVerifyEmailChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_change_with_missing_token_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = None

            request = VerificationCode(code="ABC123")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_email_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_change_with_invalid_code_raises(self, auth_service):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "REAL123", "email": "new@email.com"}
            )

            request = VerificationCode(code="WRONG")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_email_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_change_success(self, auth_service):
            mock_db_sess = AsyncMock()

            user = MagicMock()
            user.email = "old@email.com"

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "ABC123", "email": "new@email.com"}
            )

            mock_db_sess.get.return_value = user

            result = await auth_service.verify_email_change(
                VerificationCode(code="ABC123"),
                uuid4(),
                mock_db_sess,
            )

            assert result.email == "new@email.com"

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_email_change_updates_user_email(
            self, auth_service, db_sess
        ):
            email_service = auth_service._email_service
            email_service.send_email = AsyncMock()

            register_request = RegisterUserRequest(
                username="email-change-user",
                email="old@email.com",
                password="PAssword1@@1",
            )

            user = await auth_service.register_user(register_request, db_sess)
            await db_sess.commit()

            # simulate stored redis payload
            payload = {"code": "ABC123", "email": "new@email.com"}

            auth_service._redis_client.get.return_value = json.dumps(payload).encode()

            async with get_db_session() as new_db_sess:
                updated = await auth_service.verify_email_change(
                    VerificationCode(code="ABC123"),
                    user.user_id,
                    new_db_sess,
                )

                await new_db_sess.commit()

            assert updated.email == "new@email.com"


class TestRequestUsernameChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_username_change_with_missing_user_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.side_effect = [None]

            request = ChangeUsernameRequest(username="new-username")

            with pytest.raises(UserDoesNotExistException):
                await auth_service.request_username_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_username_change_with_existing_username_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            mock_user = MagicMock()
            existing_user = MagicMock()

            mock_db_sess.scalar.side_effect = [
                mock_user,  # user exists
                existing_user,  # username already taken
            ]

            request = ChangeUsernameRequest(username="taken-username")

            with pytest.raises(
                UserAlreadyExistsException,
                match="User with username 'taken-username' already exists",
            ):
                await auth_service.request_username_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_username_change_sets_redis_and_sends_email(
            self, auth_service, email_service
        ):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            mock_user = MagicMock()
            mock_user.email = "current@email.com"

            mock_db_sess.scalar.side_effect = [mock_user, None]

            auth_service._redis_client.set = AsyncMock()
            email_service.send_email = AsyncMock()

            request = ChangeUsernameRequest(username="new-username")

            await auth_service.request_username_change(
                request,
                user_id,
                mock_db_sess,
            )

            auth_service._redis_client.set.assert_awaited_once()

            redis_call_args = auth_service._redis_client.set.await_args

            assert (
                redis_call_args.args[0]
                == f"{REDIS_CHANGE_USERNAME_KEY_PREFIX}{user_id}"
            )

            payload = json.loads(redis_call_args.args[1])

            assert payload["username"] == "new-username"
            assert "code" in payload

            assert redis_call_args.kwargs["ex"] == VERIFICATION_CODE_EXPIRY_SECS

            email_service.send_email.assert_awaited_once()

            email_args = email_service.send_email.await_args.args

            assert email_args[0] == "current@email.com"
            assert email_args[1] == "Confirm Your Username Change"


class TestVerifyUsernameChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_username_change_with_missing_token_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = None

            request = VerificationCode(code="ABC123")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_username_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_username_change_with_invalid_code_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "REAL123", "username": "new-name"}
            )

            request = VerificationCode(code="WRONG")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_username_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_username_change_success(self, auth_service):
            mock_db_sess = AsyncMock()

            user = MagicMock()
            user.username = "old-name"

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "ABC123", "username": "new-name"}
            )

            mock_db_sess.get.return_value = user

            result = await auth_service.verify_username_change(
                VerificationCode(code="ABC123"),
                uuid4(),
                mock_db_sess,
            )

            assert result.username == "new-name"

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_username_change_updates_user_username(
            self, auth_service, db_sess
        ):
            email_service = auth_service._email_service
            email_service.send_email = AsyncMock()

            register_request = RegisterUserRequest(
                username="old-username",
                email="username-change@email.com",
                password="PAssword1@@1",
            )

            user = await auth_service.register_user(register_request, db_sess)
            await db_sess.commit()

            payload = {"code": "ABC123", "username": "new-username"}

            auth_service._redis_client.get.return_value = json.dumps(payload).encode()

            async with get_db_session() as new_db_sess:
                updated = await auth_service.verify_username_change(
                    VerificationCode(code="ABC123"),
                    user.user_id,
                    new_db_sess,
                )
                await new_db_sess.commit()

            assert updated.username == "new-username"


class TestRequestPasswordChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_password_change_with_missing_user_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.get.return_value = None

            request = ChangePasswordRequest(password="NewPassword123!!")

            with pytest.raises(UserDoesNotExistException):
                await auth_service.request_password_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_password_change_sets_redis_and_sends_email(
            self, auth_service, email_service
        ):
            mock_db_sess = AsyncMock()

            user_id = uuid4()

            mock_user = MagicMock()
            mock_user.email = "current@email.com"

            mock_db_sess.get.return_value = mock_user

            auth_service._redis_client.set = AsyncMock()
            email_service.send_email = AsyncMock()

            request = ChangePasswordRequest(password="NewPassword123!!")

            await auth_service.request_password_change(
                request,
                user_id,
                mock_db_sess,
            )

            auth_service._redis_client.set.assert_awaited_once()

            redis_call_args = auth_service._redis_client.set.await_args

            assert (
                redis_call_args.args[0]
                == f"{REDIS_CHANGE_PASSWORD_KEY_PREFIX}{user_id}"
            )

            payload = json.loads(redis_call_args.args[1])

            assert payload["password"] == "NewPassword123!!"
            assert "code" in payload

            assert redis_call_args.kwargs["ex"] == VERIFICATION_CODE_EXPIRY_SECS

            email_service.send_email.assert_awaited_once()

            email_args = email_service.send_email.await_args.args
            assert email_args[0] == "current@email.com"
            assert email_args[1] == "Confirm Your Password Change"


class TestVerifyPasswordChange:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_password_change_with_missing_token_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = None

            request = VerificationCode(code="ABC123")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_password_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_password_change_with_invalid_code_raises(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "REAL123", "password": "NewPassword123!"}
            )

            request = VerificationCode(code="WRONG")

            with pytest.raises(ValueError, match="Invalid or expired token"):
                await auth_service.verify_password_change(
                    request,
                    uuid4(),
                    mock_db_sess,
                )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_password_change_success(self, auth_service):
            mock_db_sess = AsyncMock()

            user = MagicMock()
            user.password = "old-hash"

            auth_service._redis_client.get.return_value = json.dumps(
                {"code": "ABC123", "password": "NewPassword123!"}
            )

            mock_db_sess.get.return_value = user

            result = await auth_service.verify_password_change(
                VerificationCode(code="ABC123"),
                uuid4(),
                mock_db_sess,
            )

            assert result.password == "NewPassword123!"

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_verify_password_change_updates_user_password(
            self, auth_service, db_sess
        ):
            email_service = auth_service._email_service
            email_service.send_email = AsyncMock()

            register_request = RegisterUserRequest(
                username="password-change-user",
                email="password-change@email.com",
                password="OldPassword123!!",
            )

            user = await auth_service.register_user(register_request, db_sess)
            await db_sess.commit()

            payload = {"code": "ABC123", "password": "NewPassword123!!"}

            auth_service._redis_client.get.return_value = json.dumps(payload)

            async with get_db_session() as new_db_sess:
                updated = await auth_service.verify_password_change(
                    VerificationCode(code="ABC123"),
                    user.user_id,
                    new_db_sess,
                )
                await new_db_sess.commit()

            assert updated.password == "NewPassword123!!"


class TestRequestResetPassword:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_reset_password_with_unknown_email_returns_response(
            self, auth_service
        ):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            request = ResetPasswordRequest(email="missing@email.com")

            result = await auth_service.request_reset_password(
                request,
                mock_db_sess,
            )

            assert isinstance(result, ResetPasswordResponse)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_request_reset_password_sets_redis_and_sends_email(
            self, auth_service, email_service
        ):
            mock_db_sess = AsyncMock()

            user = MagicMock()
            user.user_id = uuid4()

            mock_db_sess.scalar.return_value = user

            auth_service._redis_client.set = AsyncMock()
            email_service.send_email = AsyncMock()

            request = ResetPasswordRequest(email="user@email.com")

            await auth_service.request_reset_password(
                request,
                mock_db_sess,
            )

            auth_service._redis_client.set.assert_awaited_once()
            email_service.send_email.assert_awaited_once()

            redis_args = auth_service._redis_client.set.await_args

            assert redis_args.args[0].startswith(REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX)

            payload = json.loads(redis_args.args[1])
            assert payload["user_id"] == str(user.user_id)

            assert redis_args.kwargs["ex"] == REDIS_PASSWORD_RESET_EXPIRY_SECS

            email_args = email_service.send_email.await_args.args
            assert "Reset Your Password" in email_args[1]
